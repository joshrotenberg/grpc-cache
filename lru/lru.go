/*

Package lru implements an LRU+TTL caching library that (mostly) follows the
functions offered by memcached. It was initially based on groupcache/lru and
evolved into a more fully featured library to support the back end caching
needs of grpc-cache. Cache items are keyed with strings and valued with
[]byte. While interface{} values might be a bit more flexible, []byte was
chosen for a few reasons. First, []byte aligns better with protobuf's byte
type, though this library should be useful outside of that context. Second,
it's relatively easy to encode/decode Go types into a []byte, and in fact
common complex encoded types in Go use []byte natively (encoding/json,
encoding/gob, etc). Third, the update functions (Increment/Decrement and
Append/Prepend) are easier to write and test without resorting to
reflection. For convenience, helpers are provided for encoding and decoding
uint64 to/from []byte for counters.

This library employs two cache strategies: Least Recently Used (LRU) and Time
To Live. LRU can be disabled globally by setting the maxEntries to 0:

		myCache := lru.New(0)

TTL can be disabled on a per item basis by setting the ttl argument in the
various set commands to 0: Otherwise, items will expire when the time.Duration

		myCache.Set("blah", []byte("whatever"), time.Minutes*20)

has been reached and the item is accessed again ... note that timeouts are
lazy, so items that are set and have timed out but aren't accessed will count
towards the LRU max.

Note that this library is not thread safe. Locking has been left up to the
caller. This allows, for exammple, more efficient batch operations because the
library functions operate on a single value, but the caller could lock around a
set of operations. In addition, it allows the caller to use their preferred
method for assuring safe concurrent access. And finally, if the library is used
in a single-threaded situation, locking unecessary locking won't needlessly
impact performance. All that said, the Cache type conveniently embeds
sync.Mutex so that a per instance lock is easy to use:

		myCache.Lock()
		myCache.Set("thing", []byte("stuff"), 0)
		myCache.Unlock()

Note also that all operations read from and potentially write to internal data
structures, so locking in a concurrent environment is necessary, even for
Get/Gets.

*/
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
	maxEntries      int
	evictionHandler EvictionHandler
	lruList         *list.List
	cache           map[string]*list.Element
	casID           uint64
}

// entry represents a an entry in the cache.
type entry struct {
	key       string
	value     []byte
	cas       uint64
	ttl       time.Duration
	createdAt time.Time
}

// EvictionReason encapsulates the reason for an eviction in an evictionHandler
type EvictionReason int

// String stringifies the EvictionReason
func (e EvictionReason) String() string {
	switch e {
	case LRUEviction:
		return "LRUEviction"
	case TTLEviction:
		return "TTLEviction"
	}
	return ""
}

const (
	// LRUEviction denotes an item was evicted via LRU
	LRUEviction = iota
	// TTLEviction denotes an item was evicted via TTL
	TTLEviction
)

// EvictionHandler is an interface for implementing a function to be called
// when an item is evicted from the cache.
type EvictionHandler interface {
	HandleEviction(key string, value []byte, reason EvictionReason)
}

// EvictionHandlerFunc is a convenience type for for EvictionHandler
// implementations.
type EvictionHandlerFunc func(key string, value []byte, reason EvictionReason)

// HandleEviction implements the EvictionHandler interface.
func (h EvictionHandlerFunc) HandleEviction(key string, value []byte, reason EvictionReason) {
	h(key, value, reason)
}

// ErrNotFound is the error returned when a value isn't found.
var ErrNotFound = errors.New("item not found")

// ErrExists is the error returned when an item exists.
var ErrExists = errors.New("item exists")

// New creates a new Cache and initializes the various internal items.
func New(maxEntries int) *Cache {
	return &Cache{
		maxEntries: maxEntries,
		lruList:    list.New(),
		cache:      make(map[string]*list.Element),
		casID:      0,
	}
}

// WithEvictionHandler attaches a callback that will be called whenever an item
// is evicted from the cache. Note that this will be called for LRU or TTL
// evictions but not for explicit calls to Delete.
func (c *Cache) WithEvictionHandler(h EvictionHandler) *Cache {
	c.evictionHandler = h
	return c
}

// Set unconditionally sets the item, potentially overwriting a previous value
// and moving the item to the top of the LRU.
func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	// key already exists, update values and move to the front
	if ee, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(ee)
		ee.Value.(*entry).value = value
		ee.Value.(*entry).ttl = ttl
		ee.Value.(*entry).createdAt = time.Now()
		ee.Value.(*entry).cas = c.nextCasID()
		return
	}
	// new entry: create, store and update the LRU
	ele := c.lruList.PushFront(&entry{
		key:       key,
		value:     value,
		ttl:       ttl,
		createdAt: time.Now(),
		cas:       c.nextCasID(),
	})
	c.cache[key] = ele
	if c.maxEntries != 0 && c.lruList.Len() > c.maxEntries {
		ele := c.lruList.Back()
		if ele != nil {
			if c.evictionHandler != nil {
				c.evictionHandler.HandleEviction(ele.Value.(*entry).key, ele.Value.(*entry).value, LRUEviction)
			}
			c.removeElement(ele)
		}
	}
}

// Touch updates the item's eviction status (LRU and TTL if supplied) and CAS
// ID without requiring the value. If the item doesn't already exist, it
// returns an ErrNotFound.
func (c *Cache) Touch(key string, ttl time.Duration) error {
	if ele, ok := c.cache[key]; ok {
		c.Set(key, ele.Value.(*entry).value, ttl)
		return nil
	}
	return ErrNotFound
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
// it returns ErrNotFound, and if the item exists but has a different cas
// value, it returns ErrExists. CAS is useful when multiple clients may be
// operating on the same cached items and updates should only be applied by one
// if a change hasn't occurred in the meantime by another.
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

// getElement gets the raw cache entry from the map if it both exists and has
// not yet expired. If the element is still valid, it moves it to the front of
// the LRU list and returns the raw element.
func (c *Cache) getElement(key string) *list.Element {
	if ele, hit := c.cache[key]; hit {
		if isExpired(ele) {
			if c.evictionHandler != nil {
				c.evictionHandler.HandleEviction(ele.Value.(*entry).key, ele.Value.(*entry).value, TTLEviction)
			}
			c.removeElement(ele)
			return nil
		}
		c.lruList.MoveToFront(ele)
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

// Increment increments the value of key by incrementBy. The value should be stored
// as a uint64 converted to a []byte with Uint64ToBytes (or something
// equivalent) or the behavior is undefined.
func (c *Cache) Increment(key string, incrementBy uint64) error {
	ele := c.getElement(key)
	if ele != nil {
		n, err := BytesToUint64(ele.Value.(*entry).value)
		if err != nil {
			return err
		}

		n += incrementBy

		b := Uint64ToBytes(n)
		ele.Value.(*entry).value = b
		ele.Value.(*entry).cas = c.nextCasID()
		return nil

	}
	return ErrNotFound
}

// Decrement decrements the value of key by decrBy. The value should be stored
// as a uint64 converted to a []byte with Uint64ToBytes (or something
// equivalent) or the behavior is undefined.
func (c *Cache) Decrement(key string, decrementBy uint64) error {
	ele := c.getElement(key)
	if ele != nil {
		n, err := BytesToUint64(ele.Value.(*entry).value)
		if err != nil {
			return err
		}

		n -= decrementBy

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
	c.lruList = nil
	c.cache = nil
}

// nextCasId increments and returns the next cas id.
func (c *Cache) nextCasID() uint64 {
	c.casID++
	return c.casID
}

// isExpired returns true if the item exists and is expired, false otherwise.
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

// removeElement unconditionally removed the element from the cache.
func (c *Cache) removeElement(e *list.Element) {
	c.lruList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
}
