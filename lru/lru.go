// Package lru implements an LRU+TTL caching library that (mostly) follows the
// functions offered by memcached. It was initially based on groupcache/lru and
// evolved into a more fully featured library to support the back end caching
// needs of grpc-cache. Cache items are keyed with strings and valued with
// []byte. While interface{} values might be a bit more flexible, []byte was
// chosen for a few reasons. First, []byte aligns better with protobuf's byte
// type, though this library should be useful outside of that context. Second,
// it's relatively easy to encode/decode Go types into a []byte, and in fact
// common complex encoded types in Go use []byte natively (encoding/json,
// encoding/gob, etc). Third, the update functions (Increment/Decrement and
// Append/Prepend) are easier to write and test without resorting to
// reflection. For convenience, helpers are provided for encoding and decoding
// uint64 to/from []byte for counters.

package lru

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// Cache is an LRU+TTL cache
type Cache struct {
	sync.Mutex
	MaxEntries int

	ll    *list.List
	cache map[string]*list.Element
	casID uint64
}

type entry struct {
	key       string
	value     []byte
	cas       uint64
	ttl       time.Duration
	createdAt time.Time
}

// ErrNotFound is the error returned when a value isn't found.
var ErrNotFound = errors.New("item not found")

// ErrExists is the error returned when an item exists.
var ErrExists = errors.New("item exists")

// New creates a new Cache and initializes the various internal items.
func New(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[string]*list.Element),
		casID:      0,
	}
}

// Set unconditionally sets the item, potentially overwriting a previous value
// and moving the item to the top of the LRU.
func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		ee.Value.(*entry).ttl = ttl
		ee.Value.(*entry).createdAt = time.Now()
		ee.Value.(*entry).cas = c.nextCasID()
		return
	}
	ele := c.ll.PushFront(&entry{
		key:       key,
		value:     value,
		ttl:       ttl,
		createdAt: time.Now(),
		cas:       c.nextCasID(),
	})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		ele := c.ll.Back()
		if ele != nil {
			c.removeElement(ele)
		}
	}
}

// Add sets the item only if it doesn't already exist.
func (c *Cache) Add(key string, value []byte, ttl time.Duration) error {
	if _, ok := c.cache[key]; !ok {
		c.Set(key, value, ttl)
		return nil
	}
	return ErrExists
}

// Replace only sets the item if it does already exist.
func (c *Cache) Replace(key string, value []byte, ttl time.Duration) error {
	if _, ok := c.cache[key]; ok {
		c.Set(key, value, ttl)
		return nil
	}
	return ErrNotFound
}

// Cas is a "compare and swap" or "check and set" operation. It attempts to set
// the key/value pair if a) the item already exists in the cache and
// b) the item's current cas value matches the supplied argument. If the item doesn't exist,
// it returns NotFound, and if the item exists but has a different cas value, it returns Exists.
func (c *Cache) Cas(key string, value []byte, ttl time.Duration, cas uint64) error {
	if ele, ok := c.cache[key]; ok {
		if ele.Value.(*entry).cas == cas {
			c.Set(key, value, ttl)
			return nil
		}
		return ErrExists
	}
	return ErrNotFound
}

func (c *Cache) getElement(key string) *list.Element {
	if ele, hit := c.cache[key]; hit {
		if isExpired(ele) {
			c.removeElement(ele)
			return nil
		}
		c.ll.MoveToFront(ele)
		return ele
	}
	return nil
}

// Get gets the value for the given key.
func (c *Cache) Get(key string) ([]byte, error) {
	ele := c.getElement(key)
	if ele != nil {
		return ele.Value.(*entry).value, nil
	}
	return nil, ErrNotFound
}

// Gets gets the value for the given key and also returns the value's CAS ID.
func (c *Cache) Gets(key string) ([]byte, uint64, error) {
	ele := c.getElement(key)
	if ele != nil {
		return ele.Value.(*entry).value, ele.Value.(*entry).cas, nil
	}
	return nil, 0, ErrNotFound
}

// Append appends the given value to the currently stored value for the key. If
// the key doesn't currently exist (or has aged out) ErrNotFound is returned.
func (c *Cache) Append(key string, value []byte, ttl time.Duration) error {
	ele := c.getElement(key)
	if ele != nil {
		newValue := append(ele.Value.(*entry).value, value...)
		c.Set(key, newValue, ttl)
		return nil
	}
	return ErrNotFound
}

// Prepend prepends the given value to the currently stored value for the key. If
// the key doesn't currently exist (or has aged out) ErrNotFound is returned.
func (c *Cache) Prepend(key string, value []byte, ttl time.Duration) error {
	ele := c.getElement(key)
	if ele != nil {
		newValue := append(value, ele.Value.(*entry).value...)
		c.Set(key, newValue, ttl)
		return nil
	}
	return ErrNotFound
}

// BytesToUint64 is a helper to convert a byte slice to a uint64.
func BytesToUint64(b []byte) (uint64, error) {
	y, err := binary.ReadUvarint(bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	return y, nil
}

// Uint64ToBytes is a helper to convert a uint64 to a byte slice.
func Uint64ToBytes(n uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, n)
	return buf
}

// Increment increments the value of key by incrBy. The value should be stored
// as a uint64 converted to a []byte with Uint64ToBytes (or something
// equivalent) or the behavior is undefined.
func (c *Cache) Increment(key string, incrBy uint64) error {
	ele := c.getElement(key)
	if ele != nil {
		n, err := BytesToUint64(ele.Value.(*entry).value)
		if err != nil {
			return err
		}

		n += incrBy

		b := Uint64ToBytes(n)
		ele.Value.(*entry).value = b
		ele.Value.(*entry).cas = c.nextCasID()
		return nil

	}
	return ErrNotFound
}

// Decrement decrements the value of key by incrBy. The value should be stored
// as a uint64 converted to a []byte with Uint64ToBytes (or something
// equivalent) or the behavior is undefined.
func (c *Cache) Decrement(key string, incrBy uint64) error {
	ele := c.getElement(key)
	if ele != nil {
		n, err := BytesToUint64(ele.Value.(*entry).value)
		if err != nil {
			return err
		}

		n -= incrBy

		b := Uint64ToBytes(n)
		ele.Value.(*entry).value = b
		ele.Value.(*entry).cas = c.nextCasID()
		return nil

	}
	return ErrNotFound
}

// Delete deletes the item from the cache.
func (c *Cache) Delete(key string) {
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// FlushAll removes all items from the cache.
func (c *Cache) FlushAll() {
	c.ll = nil
	c.cache = nil
}

func (c *Cache) nextCasID() uint64 {
	c.casID++
	return c.casID
}

func isExpired(e *list.Element) bool {
	ttl := e.Value.(*entry).ttl
	if ttl == 0 {
		return false
	}

	createdAt := e.Value.(*entry).createdAt
	if time.Since(createdAt) > ttl {
		return true
	}
	return false
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
}
