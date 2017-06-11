package server

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"testing"

	pb "github.com/joshrotenberg/grpc-cache/cache"
	"github.com/joshrotenberg/grpc-cache/lru"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// ripped off from net/http/httptest/server.go
func newLocalListener() net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			panic(fmt.Sprintf("httptest: failed to listen on a port: %v", err))
		}
	}
	return l
}

func testSetup(maxEntries int) (*grpc.Server, pb.CacheClient) {

	c := lru.New(maxEntries)
	s := grpc.NewServer()
	cs := &CacheServer{c: c, s: s}
	pb.RegisterCacheServer(s, cs)
	//reflection.Register(s)
	l := newLocalListener()
	go s.Serve(l)

	conn, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error connecting to server: %s", err)
	}

	cc := pb.NewCacheClient(conn)
	return s, cc
}

func createCacheItem(key string, value string, ttl uint64) *pb.CacheItem {
	item := &pb.CacheItem{
		Key:   key,
		Value: []byte(value),
		Ttl:   ttl,
	}
	return item
}

func createCacheRequest(operation pb.CacheRequest_Operation, item *pb.CacheItem) *pb.CacheRequest {

	return &pb.CacheRequest{Operation: operation, Item: item}
}

func testSet(t *testing.T, cc pb.CacheClient, key string, value string) *pb.CacheResponse {
	setRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: key, Value: []byte(value)}}
	setResponse, err := cc.Set(context.Background(), setRequest)
	if err != nil {
		t.Fatalf("error setting item: %s", err)
	}
	return setResponse
}

func testGet(t *testing.T, cc pb.CacheClient, key string, value string) *pb.CacheResponse {
	getRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: key}}
	getResponse, err := cc.Get(context.Background(), getRequest)
	if value == "" && err == nil {
		t.Fatalf("what")
	}
	if thing, ok := status.FromError(err); ok {
		t.Logf("code is %#v", thing.Code())
	}

	t.Log(getResponse, err)
	return getResponse
}

func TestCache(t *testing.T) {

	_, cc := testSetup(20)
	//defer s.GracefulStop()

	setRes := testSet(t, cc, "foo", "bar")
	t.Log(setRes)
	testGet(t, cc, "foo", "bar")

}

func TestSet(t *testing.T) {
	_, cc := testSetup(20)
	//defer s.GracefulStop()

	setRequest := createCacheRequest(pb.CacheRequest_SET, &pb.CacheItem{Key: "foo", Value: []byte("bar")})
	_, err := cc.Set(context.Background(), setRequest)
	if err != nil {
		t.Fatalf("error setting item: %s", err)
	}

	getRequest := createCacheRequest(pb.CacheRequest_GET, &pb.CacheItem{Key: "foo"})
	getResponse, err := cc.Get(context.Background(), getRequest)
	if err != nil {
		t.Fatalf("error getting item: %s", err)
	}
	item := getResponse.Item
	if bytes.Compare(item.Value, []byte("bar")) != 0 {
		t.Fatalf("cache item value wasn't as expected; got %s, expected %s", item.Value, []byte("bar"))
	}
}

func TestCAS(t *testing.T) {

	_, cc := testSetup(20)
	//defer s.GracefulStop()

	// set something initially
	setRequest := createCacheRequest(pb.CacheRequest_SET, &pb.CacheItem{Key: "foo", Value: []byte("bar")})
	_, err := cc.Set(context.Background(), setRequest)
	if err != nil {
		t.Fatalf("error setting item: %s", err)
	}

	// now get it along with the cas value
	getsRequest := createCacheRequest(pb.CacheRequest_GETS, &pb.CacheItem{Key: "foo"})
	getsResponse, err := cc.Gets(context.Background(), getsRequest)
	if err != nil {
		t.Fatalf("error getting item: %s", err)
	}

	item := getsResponse.Item
	if bytes.Compare(item.Value, []byte("bar")) != 0 {
		t.Fatalf("cache item value wasn't as expected; got %s, expected %s", item.Value, []byte("bar"))
	}
	// this is the first thing we set in this cache so we know the cas should be 1
	if item.Cas != 1 {
		t.Fatalf("cas should have been 1 but it was %d", item.Cas)
	}

	// set something again with the same key and provide the cas value
	casRequest := createCacheRequest(pb.CacheRequest_CAS, &pb.CacheItem{Key: "foo", Value: []byte("bluh"), Cas: 1})
	casResponse, err := cc.Cas(context.Background(), casRequest)
	if err != nil {
		t.Fatalf("error CASing item: %s", err)
	}
	item = casResponse.Item

	// pull it back out again
	getsRequest = createCacheRequest(pb.CacheRequest_GETS, &pb.CacheItem{Key: "foo"})
	getsResponse, err = cc.Gets(context.Background(), getsRequest)
	if err != nil {
		t.Fatalf("error getting item: %s", err)
	}

	item = getsResponse.Item
	if bytes.Compare(item.Value, []byte("bluh")) != 0 {
		t.Fatalf("cache item value wasn't as expected; got %s, expected %s", item.Value, []byte("bluh"))
	}
	// the cas should have been incremented
	if item.Cas != 2 {
		t.Fatalf("cas should have been 2 but it was %d", item.Cas)
	}

	// now try to set something again with the key but provide a cas that definitely isn't right. the set should fail
	casRequest = createCacheRequest(pb.CacheRequest_CAS, &pb.CacheItem{Key: "foo", Value: []byte("nope"), Cas: 9})
	casResponse, err = cc.Cas(context.Background(), casRequest)
	if err == nil {
		t.Fatal("cas shoud have failed")
	}
}

func TestAdd(t *testing.T) {

	_, cc := testSetup(20)
	//defer s.Stop()

	addRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo", Value: []byte("bar")}}
	_, err := cc.Add(context.Background(), addRequest)
	if err != nil {
		t.Fatalf("error adding item: %s", err)
	}

	addRequest = createCacheRequest(pb.CacheRequest_ADD, &pb.CacheItem{Key: "foo", Value: []byte("bar")})
	_, err = cc.Add(context.Background(), addRequest)
	if err == nil {
		t.Fatal("add should have failed")
	}
}

func TestReplace(t *testing.T) {
	_, cc := testSetup(20)
	//defer s.GracefulStop()

	replaceRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo", Value: []byte("bar")}}
	_, err := cc.Replace(context.Background(), replaceRequest)
	t.Logf("%#v", err)
	if err == nil {
		t.Fatalf("expected an error trying to replace an item that doesn't exist")
	}
	testSet(t, cc, "foo", "bar")
	replaceRequest = &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo", Value: []byte("rar")}}
	_, err = cc.Replace(context.Background(), replaceRequest)
	if err != nil {
		t.Fatalf("got an error trying to replace an item that should exist: %s", err)
	}
	getResponse := testGet(t, cc, "foo", "rar")
	t.Log(getResponse)
}

/*
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
	t.Logf("%#v", c)

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

	replaceItem := createCacheItem("foo", "newbar", 0)
	replace, err := c.Replace(context.Background(), &pb.ReplaceRequest{Item: replaceItem})
	if err != nil {
		t.Fatal("Got an error replacing 'foo': %s", err)
	}
	t.Logf("Replace: %s", replace)

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
	done <- true
}
*/
