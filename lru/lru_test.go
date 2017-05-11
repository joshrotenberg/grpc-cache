package lru

import (
	"bytes"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	// create a cache with 3 max entries
	c := New(3)

	// set a single item. it should be there
	c.Set("foo", []byte("bar"), 0)
	if v, ok := c.Get("foo"); ok {
		if bytes.Compare(v, []byte("bar")) != 0 {
			t.Fatalf("stored value not as expected: %s", v)
		}
	}

	// add the same key, it shouldn't replace the old value since it already exists
	c.Add("foo", []byte("notbar"), 0)
	if v, ok := c.Get("foo"); ok {
		if bytes.Compare(v, []byte("bar")) != 0 {
			t.Fatalf("foo should still be bar, not 'notbar'")
		}
	}

	// use replace, it should overwrite the old value
	c.Replace("foo", []byte("newbar"), 0)
	if v, ok := c.Get("foo"); ok {
		if bytes.Compare(v, []byte("newbar")) != 0 {
			t.Fatalf("foo should now be 'newbar' but its %s", v)
		}
	}

	// try to replace something that doesn't exist, it shouldn't add anything
	c.Replace("yuk", []byte("doesn'tmatter"), 0)
	if v, ok := c.Get("yuk"); ok != false {
		t.Fatalf("foo should now be 'newbar' but its %s", v)
	}

	// now push our first item "foo" off the LRU by adding three new items
	c.Add("bar", []byte("stuff"), 0)
	c.Add("thing", []byte("other"), 0)
	c.Add("another", []byte("thing"), 0)
	if v, ok := c.Get("foo"); ok != false {
		t.Fatalf("'foo' should be gone now but it still returned a value: %s", v)
	}

	// delete something
	c.Delete("bar")
	if v, ok := c.Get("bar"); ok != false {
		t.Fatalf("'bar' should have been deleted: %s", v)
	}

	// clear the entire cache
	c.FlushAll()
	if v, ok := c.Get("thing"); ok != false {
		t.Fatalf("all items should have been flushed but found: %", v)
	}
}

func TestTTL(t *testing.T) {
	c := New(5)

	c.Set("foo", []byte("bar"), time.Second*3)
	time.Sleep(time.Second * 1)

	if _, ok := c.Get("foo"); !ok {
		t.Fatal("'foo' should still be there")
	}

	time.Sleep(time.Second * 2)
	if _, ok := c.Get("foo"); ok {
		t.Fatal("'foo' should have expired")
	}

}
