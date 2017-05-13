package lru

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	// create a cache with 3 max entries
	c := New(3)

	// set a single item. it should be there
	c.Set("foo", []byte("bar"), 0)
	if v, err := c.Get("foo"); err == nil {
		if bytes.Compare(v, []byte("bar")) != 0 {
			t.Fatalf("stored value not as expected: %s", v)
		}
	}

	// add the same key, it shouldn't replace the old value since it already exists
	err := c.Add("foo", []byte("notbar"), 0)
	if err != ErrExists {
		t.Fatal("expected an Exists error")
	}
	if v, err := c.Get("foo"); err == nil {
		if bytes.Compare(v, []byte("bar")) != 0 {
			t.Fatalf("foo should still be bar, not 'notbar'")
		}
	}

	// use replace, it should overwrite the old value
	err = c.Replace("foo", []byte("newbar"), 0)
	if err != nil {
		t.Fatal("got an unexpected error from Replace")
	}
	if v, err := c.Get("foo"); err == nil {
		if bytes.Compare(v, []byte("newbar")) != 0 {
			t.Fatalf("foo should now be 'newbar' but its %s", v)
		}
	}

	// try to replace something that doesn't exist, it shouldn't add anything
	err = c.Replace("yuk", []byte("doesn'tmatter"), 0)
	if err != ErrNotFound {
		t.Fatal("should have gotten a NotFound error from Replace")
	}
	if v, err := c.Get("yuk"); err != ErrNotFound {
		t.Fatalf("yuk shouldn't be there but it was %s", v)
	}

	// now push our first item "foo" off the LRU by adding three new items
	err = c.Add("bar", []byte("stuff"), 0)
	if err != nil {
		t.Fatal("Add returned an error")
	}
	err = c.Add("thing", []byte("other"), 0)
	if err != nil {
		t.Fatal("Add returned an error")
	}
	err = c.Add("another", []byte("thing"), 0)
	if err != nil {
		t.Fatal("Add returned an error")
	}

	if v, err := c.Get("foo"); err != ErrNotFound {
		t.Fatalf("'foo' should be gone now but it still returned a value: %s", v)
	}

	c.Delete("bar")
	if v, err := c.Get("bar"); err != ErrNotFound {
		t.Fatalf("'bar' should have been deleted: %s", v)
	}

	// clear the entire cache
	c.FlushAll()
	if v, err := c.Get("thing"); err != ErrNotFound {
		t.Fatalf("all items should have been flushed but found: %s", v)
	}
}

func TestCAS(t *testing.T) {
	c := New(5)

	// set an initial item
	c.Set("castest", []byte("stuff"), 0)

	// fetch it with gets and get the cas value
	v, n, err := c.Gets("castest")
	if err != nil {
		t.Fatal("error getting castest")
	}
	if n != 1 || bytes.Compare(v, []byte("stuff")) != 0 {
		t.Fatal("stored value doesn't look right")
	}

	// set something new using the cas value, should succeed
	err = c.Cas("castest", []byte("new stuff"), 0, n)
	if err != nil {
		t.Fatal(err)
	}

	// set something new using the wrong cas value, should fail
	err = c.Cas("castest", []byte("nope"), 0, 100)
	if err != ErrExists {
		t.Fatal(err)
	}

	// gets the value again, should be from the first attempt
	v, n, err = c.Gets("castest")
	if err != nil {
		t.Fatal("error getting castest")
	}
	if n != 2 || bytes.Compare(v, []byte("new stuff")) != 0 {
		t.Fatal("stored value doesn't look right")
	}

	err = c.Cas("castest", []byte("yup"), 0, 2)
	if err != nil {
		t.Fatal(err)
	}

}

func TestPrependAppend(t *testing.T) {
	c := New(0)

	c.Set("foo", []byte("bar"), 0)
	c.Set("yuk", []byte("woof"), 0)

	err := c.Append("foo", []byte("stuff"), 0)
	if err != nil {
		t.Fatal("got an error trying to append")
	}

	if v, err := c.Get("foo"); err != nil || bytes.Compare(v, []byte("barstuff")) != 0 {
		t.Fatalf("append failed: %s %s", v, err)
	}

	err = c.Prepend("yuk", []byte("things"), 0)
	if err != nil {
		t.Fatalf("got an error trying to prepend")
	}

	if v, err := c.Get("yuk"); err != nil || bytes.Compare(v, []byte("thingswoof")) != 0 {
		t.Fatalf("prepend failed: %s %s", v, err)
	}
}

func TestScratch(t *testing.T) {
	v := []byte("one")
	t.Log(string(append(v, []byte("two")...)))

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, 20)
	x, err := binary.ReadUvarint(bytes.NewReader(buf))
	if err != nil {
		t.Fatal("oops")
	}
	if x != 20 {
		t.Fatal("doh")
	}
	c := New(2)
	c.Lock()
	c.Unlock()
}

func TestEvictionHandler(t *testing.T) {
	var x string
	c := New(2).WithEvictionHandler(EvictionHandlerFunc(func(key string, value []byte, reason EvictionReason) {
		t.Logf("evicted %s: %s", key, reason)
		x = key
	}))

	c.Set("first", []byte("gets evicted"), 0)
	c.Set("second", []byte("val"), time.Nanosecond*1)
	c.Set("third", []byte("val"), 0)
	if x != "first" {
		t.Fatal("expected 'first' to get evicted via LRU")
	}
	// call Get to force a ttl eviction
	v, err := c.Get("second")
	if err != ErrNotFound {
		t.Fatalf("found 'second', it should have been evicted: %s %s", err, v)
	}
	if x != "second" {
		t.Fatal("expected 'second' to get evicted via TTL")
	}
}

func TestBytesToUint64ToBytes(t *testing.T) {
	x := Uint64ToBytes(1 << 6)
	y, err := BytesToUint64(x)
	if err != nil {
		t.Fatalf("error converting bytes to uint64: %s", err)
	}
	if y != 64 {
		t.Fatalf("conversion didn't work as expected, y was %d", y)
	}
}

func TestIncrementDecrement(t *testing.T) {
	c := New(20)

	// set foo to 20
	r := Uint64ToBytes(20)
	c.Set("foo", r, 0)

	// increment foo by 20
	err := c.Increment("foo", 20)
	if err != nil {
		t.Fatalf("error trying to increment foo: %s", err)
	}

	// pull foo back out
	v, err := c.Get("foo")
	if err != nil {
		t.Fatalf("error getting 'foo': %s", err)
	}

	// convert the value
	y, err := BytesToUint64(v)
	if err != nil {
		t.Fatalf("error converting bytes to uint64: %s", err)
	}

	// and make sure it incremented
	if y != 40 {
		t.Fatalf("increment didn't work as expected, foo is %d", y)
	}

	// now derement it by 10
	err = c.Decrement("foo", 10)
	if err != nil {
		t.Fatalf("error trying to decrement foo: %s", err)
	}

	// pull foo back out
	x, err := c.Get("foo")
	if err != nil {
		t.Fatalf("error getting 'foo': %s", err)
	}

	// convert the value
	z, err := BytesToUint64(x)
	if err != nil {
		t.Fatalf("error converting bytes to uint64: %s", err)
	}

	// and make sure it decremented
	if z != 30 {
		t.Fatalf("decrement didn't work as expected, foo is %d", y)
	}
}

func TestTTL(t *testing.T) {
	c := New(5)

	c.Set("foo", []byte("bar"), time.Second*2)
	time.Sleep(time.Second * 1)

	if _, err := c.Get("foo"); err == ErrNotFound {
		t.Fatal("'foo' should still be there")
	}

	time.Sleep(time.Second * 1)
	if _, err := c.Get("foo"); err != ErrNotFound {
		t.Fatal("'foo' should have expired")
	}
}
