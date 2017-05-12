package lru

import (
	"container/list"
	"errors"
	"log"
	"time"
)

// based on groupcache/lru but with more memcached-like semantics

// Cache is an LRU+TTL cache
type Cache struct {
	MaxEntries int

	ll    *list.List
	cache map[interface{}]*list.Element
	casId uint64
}

type entry struct {
	key       string
	value     interface{}
	cas       uint64
	ttl       time.Duration
	createdAt time.Time
}

var NotFound = errors.New("item not found")
var Exists = errors.New("item exists")

// New creates a new Cache and initializes the various internal items. You *must*
// call this first.
func New(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
		casId:      0,
	}
}

// Set unconditionally sets the item, potentially overwriting a previous value
// and moving the item to the top of the LRU.
func (c *Cache) Set(key string, value interface{}, ttl time.Duration) {
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		ee.Value.(*entry).ttl = ttl
		ee.Value.(*entry).createdAt = time.Now()
		ee.Value.(*entry).cas = c.nextCasId()
		return
	}
	ele := c.ll.PushFront(&entry{
		key:       key,
		value:     value,
		ttl:       ttl,
		createdAt: time.Now(),
		cas:       c.nextCasId(),
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
func (c *Cache) Add(key string, value interface{}, ttl time.Duration) error {
	if _, ok := c.cache[key]; !ok {
		c.Set(key, value, ttl)
		return nil
	}
	return Exists
}

// Replace only sets the item if it does already exist.
func (c *Cache) Replace(key string, value interface{}, ttl time.Duration) error {
	if _, ok := c.cache[key]; ok {
		c.Set(key, value, ttl)
		return nil
	}
	return NotFound
}

// Cas is a "compare and swap" or "check and set" operation. It attempts to set
// the key/value pair if a) the item already exists in the cache and
// b) the item's current cas value matches the supplied argument. If the item doesn't exist,
// it returns NotFound, and if the item exists but has a different cas value, it returns Exists.
func (c *Cache) Cas(key string, value interface{}, ttl time.Duration, cas uint64) error {
	if ele, ok := c.cache[key]; ok {
		if ele.Value.(*entry).cas == cas {
			c.Set(key, value, ttl)
			return nil
		}
		return Exists
	}
	return NotFound
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
func (c *Cache) Get(key string) (interface{}, error) {
	ele := c.getElement(key)
	if ele != nil {
		return ele.Value.(*entry).value, nil
	}
	return nil, NotFound
}

func (c *Cache) Gets(key string) (interface{}, uint64, error) {
	ele := c.getElement(key)
	if ele != nil {
		return ele.Value.(*entry).value, ele.Value.(*entry).cas, nil
	}
	return nil, 0, NotFound
}

// Delete an item from the cache
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

func (c *Cache) nextCasId() uint64 {
	c.casId++
	return c.casId
}

func isExpired(e *list.Element) bool {
	ttl := e.Value.(*entry).ttl
	if ttl == 0 {
		return false
	}

	createdAt := e.Value.(*entry).createdAt
	if time.Since(createdAt) > ttl {
		log.Println(time.Since(createdAt))
		return true
	}
	return false
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
}
