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
	"google.golang.org/grpc/codes"
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

func testSetup(maxEntries int) pb.CacheClient {
	listener := newLocalListener()
	s2 := NewWithListener(listener, maxEntries)
	s2.Start()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error connecting to server: %s", err)
	}

	cc := pb.NewCacheClient(conn)
	return cc
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

func testGet(t *testing.T, cc pb.CacheClient, key string, expectedValue string, expectedCode codes.Code) *pb.CacheResponse {
	getRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: key}}
	getResponse, err := cc.Get(context.Background(), getRequest)
	if err != nil {
		status, ok := status.FromError(err)
		if !ok {
			t.Fatalf("unknown error: %v", err)
		}
		if status.Code() != expectedCode {
			t.Fatalf("error codes do not match: %v", status)
		}
	} else {
		if string(getResponse.Item.Value) != expectedValue {
			t.Fatalf("get: got %s, expected %s", getResponse.Item.Value, expectedValue)
		}
		return getResponse
	}
	return nil
}

func TestCache(t *testing.T) {

	cc := testSetup(20)
	//defer s.GracefulStop()

	setRes := testSet(t, cc, "foo", "bar")
	t.Log(setRes)
	testGet(t, cc, "foo", "bar", codes.OK)

}
func TestSet(t *testing.T) {
	cc := testSetup(20)
	//defer s.GracefulStop()

	setRequest := createCacheRequest(pb.CacheRequest_SET, &pb.CacheItem{Key: "foo", Value: []byte("bar")})
	_, err := cc.Set(context.Background(), setRequest)
	if err != nil {
		t.Fatalf("error setting item: %s", err)
	}
	testGet(t, cc, "foo", "bar", codes.OK)
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

	cc := testSetup(20)
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

	cc := testSetup(20)
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
	cc := testSetup(20)
	//defer s.GracefulStop()
	replaceRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo", Value: []byte("bar")}}
	_, err := cc.Replace(context.Background(), replaceRequest)

	if err == nil {
		t.Fatalf("expected an error trying to replace an item that doesn't exist")
	}
	testSet(t, cc, "foo", "bar")
	replaceRequest = &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo", Value: []byte("rar")}}
	_, err = cc.Replace(context.Background(), replaceRequest)
	if err != nil {
		t.Fatalf("got an error trying to replace an item that should exist: %s", err)
	}
	testGet(t, cc, "foo", "rar", codes.OK)
}

func TestDelete(t *testing.T) {
	cc := testSetup(20)
	testSet(t, cc, "doof", "cha")
	testGet(t, cc, "doof", "cha", codes.OK)

	deleteRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "doof"}}
	_, err := cc.Delete(context.Background(), deleteRequest)
	if err != nil {
		t.Fatalf("got an error deleting an item: %s", err)
	}

	testGet(t, cc, "doof", "", codes.NotFound)
}

func TestAppend(t *testing.T) {
	cc := testSetup(20)
	testSet(t, cc, "doof", "cha")
	appendRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "doof"}, Append: []byte("yeah")}
	cc.Append(context.Background(), appendRequest)
	testGet(t, cc, "doof", "chayeah", codes.OK)
}

func TestPrepend(t *testing.T) {
	cc := testSetup(20)
	testSet(t, cc, "doof", "cha")
	prependRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "doof"}, Prepend: []byte("yeah")}
	cc.Prepend(context.Background(), prependRequest)
	testGet(t, cc, "doof", "yeahcha", codes.OK)
}

func TestIncrement(t *testing.T) {
	cc := testSetup(20)
	v := lru.Uint64ToBytes(20)
	setRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo", Value: v}}
	_, err := cc.Set(context.Background(), setRequest)
	if err != nil {
		t.Fatal("error setting value")
	}
	getRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo"}}
	getResponse, err := cc.Get(context.Background(), getRequest)

	v2, err := lru.BytesToUint64(getResponse.Item.Value)
	if err != nil {
		t.Fatalf("error converting value: %s", err)
	}
	if v2 != 20 {
		t.Fatalf("value wasn't as expected: %d != 20", v2)
	}

	incrementRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo"}, Increment: 23}
	cc.Increment(context.Background(), incrementRequest)

	getRequest = &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo"}}
	getResponse, err = cc.Get(context.Background(), getRequest)
	v3, err := lru.BytesToUint64(getResponse.Item.Value)
	if err != nil {
		t.Fatalf("error converting value: %s", err)
	}
	if v3 != 43 {
		t.Fatalf("value wasn't as expected: %d != 43", v3)
	}
}

func TestDecrement(t *testing.T) {
	cc := testSetup(20)
	v := lru.Uint64ToBytes(43)
	setRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo", Value: v}}
	_, err := cc.Set(context.Background(), setRequest)
	if err != nil {
		t.Fatal("error setting value")
	}
	getRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo"}}
	getResponse, err := cc.Get(context.Background(), getRequest)
	v2, err := lru.BytesToUint64(getResponse.Item.Value)
	if err != nil {
		t.Fatalf("error converting value: %s", err)
	}
	if v2 != 43 {
		t.Fatalf("value wasn't as expected: %d != 43", v2)
	}

	decrementRequest := &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo"}, Decrement: 23}
	cc.Decrement(context.Background(), decrementRequest)

	getRequest = &pb.CacheRequest{Item: &pb.CacheItem{Key: "foo"}}
	getResponse, err = cc.Get(context.Background(), getRequest)
	v3, err := lru.BytesToUint64(getResponse.Item.Value)
	if err != nil {
		t.Fatalf("error converting value: %s", err)
	}
	if v3 != 20 {
		t.Fatalf("value wasn't as expected: %d != 20", v3)
	}
}

func TestFlushAll(t *testing.T) {
	cc := testSetup(20)
	testSet(t, cc, "foo", "bar")
	testSet(t, cc, "boo", "far")

	flushAllRequest := &pb.CacheRequest{}
	flushAllResponse, err := cc.FlushAll(context.Background(), flushAllRequest)
	if err != nil {
		t.Fatalf("flush all failed: %v", err)
	}
	t.Log(flushAllResponse)
}
