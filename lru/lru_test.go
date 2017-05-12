package lru

import (
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	// create a cache with 3 max entries
	c := New(3)

	// set a single item. it should be there
	c.Set("foo", "bar", 0)
	if v, err := c.Get("foo"); err == nil {
		if v != "bar" {
			t.Fatalf("stored value not as expected: %s", v)
		}
	}

	// add the same key, it shouldn't replace the old value since it already exists
	err := c.Add("foo", "notbar", 0)
	if err != Exists {
		t.Fatal("expected an Exists error")
	}
	if v, err := c.Get("foo"); err == nil {
		if v != "bar" {
			t.Fatalf("foo should still be bar, not 'notbar'")
		}
	}

	// use replace, it should overwrite the old value
	err = c.Replace("foo", "newbar", 0)
	if err != nil {
		t.Fatal("got an unexpected error from Replace")
	}
	if v, err := c.Get("foo"); err == nil {
		if v != "newbar" {
			t.Fatalf("foo should now be 'newbar' but its %s", v)
		}
	}

	// try to replace something that doesn't exist, it shouldn't add anything
	err = c.Replace("yuk", "doesn'tmatter", 0)
	if err != NotFound {
		t.Fatal("should have gotten a NotFound error from Replace")
	}
	if v, err := c.Get("yuk"); err != NotFound {
		t.Fatalf("yuk shouldn't be there but it was %s", v)
	}

	// now push our first item "foo" off the LRU by adding three new items
	err = c.Add("bar", "stuff", 0)
	if err != nil {
		t.Fatal("Add returned an error")
	}
	err = c.Add("thing", "other", 0)
	if err != nil {
		t.Fatal("Add returned an error")
	}
	err = c.Add("another", "thing", 0)
	if err != nil {
		t.Fatal("Add returned an error")
	}

	if v, err := c.Get("foo"); err != NotFound {
		t.Fatalf("'foo' should be gone now but it still returned a value: %s", v)
	}

	c.Delete("bar")
	if v, err := c.Get("bar"); err != NotFound {
		t.Fatalf("'bar' should have been deleted: %s", v)
	}

	// clear the entire cache
	c.FlushAll()
	if v, err := c.Get("thing"); err != NotFound {
		t.Fatalf("all items should have been flushed but found: %", v)
	}
}

func TestCAS(t *testing.T) {
	c := New(5)

	// set an initial item
	c.Set("castest", "stuff", 0)

	// fetch it with gets and get the cas value
	v, n, err := c.Gets("castest")
	if err != nil {
		t.Fatal("error getting castest")
	}
	if n != 1 || v != "stuff" {
		t.Fatal("stored value doesn't look right")
	}

	// set something new using the cas value, should succeed
	err = c.Cas("castest", "new stuff", 0, n)
	if err != nil {
		t.Fatal(err)
	}

	// set something new using the wrong cas value, should fail
	err = c.Cas("castest", "nope", 0, 100)
	if err != Exists {
		t.Fatal(err)
	}

	// gets the value again, should be from the first attempt
	v, n, err = c.Gets("castest")
	if err != nil {
		t.Fatal("error getting castest")
	}
	if n != 2 || v != "new stuff" {
		t.Fatal("stored value doesn't look right")
	}

	err = c.Cas("castest", "yup", 0, 2)
	if err != nil {
		t.Fatal(err)
	}

}

func TestTTL(t *testing.T) {
	c := New(5)

	c.Set("foo", []byte("bar"), time.Second*2)
	time.Sleep(time.Second * 1)

	if _, err := c.Get("foo"); err == NotFound {
		t.Fatal("'foo' should still be there")
	}

	time.Sleep(time.Second * 1)
	if _, err := c.Get("foo"); err != NotFound {
		t.Fatal("'foo' should have expired")
	}

}
