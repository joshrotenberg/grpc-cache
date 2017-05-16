package server

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/joshrotenberg/grpc-cache/cache"
	"google.golang.org/grpc"
)

func createCacheItem(key string, value string, ttl int64) *pb.CacheItem {

	item := &pb.CacheItem{
		Key:   key,
		Value: []byte(value),
		Ttl:   ttl,
	}
	return item
}

func TestCacheServer(t *testing.T) {

	done := make(chan bool)
	cs := NewCacheServer(4096)
	go func() {
		cs.Start("localhost:5051")
		<-done
		cs.Stop()
	}()

	conn, err := grpc.Dial("localhost:5051", grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Couldn't connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal("error closing connection")
		}
	}()
	c := pb.NewCacheClient(conn)

	setItem := createCacheItem("foo", "bar", 0)
	set, err := c.Set(context.Background(), &pb.SetRequest{Item: setItem})
	if err != nil {
		t.Fatalf("Couldn't add: %s", err)
	}
	t.Logf("Set: %s", set)

	getItem := createCacheItem("foo", "", 0)
	get, err := c.Get(context.Background(), &pb.GetRequest{Item: getItem})
	if err != nil {
		t.Fatalf("Couldn't get: %s", err)
	}
	if bytes.Compare(get.Item.Value, []byte("bar")) != 0 {
		t.Fatalf("Get value returned something weird: %s", get.Item.Value)
	}
	t.Logf("Get: %s", get)

	addItem := createCacheItem("foo", "notbar", 0)
	add, err := c.Add(context.Background(), &pb.AddRequest{Item: addItem})
	if err == nil {
		t.Fatal("Expected an error adding 'foo'")
	}
	t.Logf("Add: %s %#v", add, err)

	/*
		// test Get
		get, err := c.Get(context.Background(), &pb.GetRequest{Key: "foo"})
		if err != nil {
			t.Fatalf("Couldn't get: %v", err)
		}
		if get.Ok != true {
			t.Fatalf("Expected to find key/value pair")
		}

		t.Logf("Got: %s for %s", get.Value, add.Key)

		// test Get with an item that doesn't  exist
		missing, err := c.Get(context.Background(), &pb.GetRequest{Key: "bar"})
		if err != nil {
			t.Fatalf("Error getting missing key/value pair: %v", err)
		}
		if missing.Ok != false {
			t.Fatalf("Expected key/value pair to be missing")
		}

		// test Remove
		remove, err := c.Remove(context.Background(), &pb.RemoveRequest{Key: "foo"})
		if err != nil {
			t.Fatalf("Error removing item: %v", err)
		}
		t.Logf("Removed: %s", remove.Key)

		// test Len
		len, err := c.Len(context.Background(), &pb.LenRequest{})
		if err != nil {
			t.Fatal("Error getting length")
		}
		if len.Len != 0 {
			t.Fatal("Expected an empty cache")
		}
		t.Logf("Cache length: %d", len.Len)

		// add something again
		add, err = c.Add(context.Background(), &pb.AddRequest{Key: "woof", Value: []byte("yuk")})
		if err != nil {
			t.Fatalf("Couldn't add: %v", err)
		}

		len, err = c.Len(context.Background(), &pb.LenRequest{})
		if err != nil {
			t.Fatal("Error getting length")
		}
		if len.Len != 1 {
			t.Fatal("Expected a single item")
		}
		t.Logf("Cache length: %d", len.Len)

		_, err = c.Clear(context.Background(), &pb.ClearRequest{})
		if err != nil {
			t.Fatal("Error clearing the cache")
		}
		t.Logf("Cleared the cache")

		// test Len
		len, err = c.Len(context.Background(), &pb.LenRequest{})
		if err != nil {
			t.Fatal("Error getting length")
		}
		if len.Len != 0 {
			t.Fatal("Expected an empty cache")
		}
		t.Logf("Cache length: %d", len.Len)
	*/
	done <- true
}
