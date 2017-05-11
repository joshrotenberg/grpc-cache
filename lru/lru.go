package lru

import (
	"container/list"
	"log"
	"sync"
	"time"
)

// based on groupcache/lru but with internal locking, ttl support,
// and byte key/values

// Cache is an LRU+TTL cache
type Cache struct {
	MaxEntries int

	ll    *list.List
	cache map[interface{}]*list.Element
	mutex *sync.Mutex
	casId uint64
}

type entry struct {
	key       string
	value     []byte
	cas       uint64
	ttl       time.Duration
	createdAt time.Time
}

// New creates a new Cache and initializes the various internal items. You *must*
// call this first.
func New(maxEntries int) *Cache {
	var x uint64 = 0
	log.Println(x)
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
		mutex:      &sync.Mutex{},
		casId:      0,
	}
}

// Set unconditionally sets the item, potentially overwriting a previous value
// and moving the item to the top of the LRU.
func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.casId++
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		ee.Value.(*entry).ttl = ttl
		ee.Value.(*entry).createdAt = time.Now()
		ee.Value.(*entry).cas = c.casId
		return
	}
	ele := c.ll.PushFront(&entry{
		key:       key,
		value:     value,
		ttl:       ttl,
		createdAt: time.Now(),
		cas:       c.casId,
	})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

// Add sets the item only if it doesn't already exist.
func (c *Cache) Add(key string, value []byte, ttl time.Duration) {
	if _, ok := c.cache[key]; !ok {
		c.Set(key, value, ttl)
	}
	return
}

// Replace only sets the item if it does already exist.
func (c *Cache) Replace(key string, value []byte, ttl time.Duration) {
	if _, ok := c.cache[key]; ok {
		c.Set(key, value, ttl)
	}
	return
}

// Get gets the value for the given key.
func (c *Cache) Get(key string) ([]byte, bool) {
	if ele, hit := c.cache[key]; hit {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if isExpired(ele) {
			c.removeElement(ele)
			return nil, false
		}
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return nil, false
}

// Delete an item from the cache
func (c *Cache) Delete(key string) {
	c.mutex.Lock()
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
	c.mutex.Unlock()
}

// FlushAll removes all items from the cache.
func (c *Cache) FlushAll() {
	c.mutex.Lock()
	c.ll = nil
	c.cache = nil
	c.mutex.Unlock()
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
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
