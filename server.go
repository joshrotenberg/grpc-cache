package server

import (
	"log"
	"net"
	"time"

	pb "github.com/joshrotenberg/grpc-cache/cache"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/joshrotenberg/grpc-cache/lru"
)

// CacheServer encapsulates the cache server things.
type CacheServer struct {
	c *lru.Cache
	s *grpc.Server
	l *net.Listener
}

// NewCacheServer returns a new instance of the server. It takes a maxEntries argument,
// which defines the max number of entries allowed in the cache. Set to 0 for unlimited.
func NewCacheServer(maxEntries int) *CacheServer {

	c := lru.New(maxEntries)
	s := grpc.NewServer()
	cs := &CacheServer{c: c, s: s}
	pb.RegisterCacheServer(s, cs)
	reflection.Register(s)
	return cs
}

// Start starts the cache server. It takes a host:port string on which to run the server.
func (s *CacheServer) Start(host string) {
	lis, err := net.Listen("tcp", host)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := s.s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
}

// Stop tries to gracefull stop the server.
func (s *CacheServer) Stop() {
	s.s.GracefulStop()
}

func cacheError(err error, op pb.CacheRequest_Operation, key string) error {
	switch err {
	case lru.ErrNotFound:
		return status.Errorf(codes.NotFound, "%s error: '%s' not found", op, key)
	case lru.ErrExists:
		return status.Errorf(codes.AlreadyExists, "%s error: '%s' exists", op, key)
	}
	return err
}

func cacheResponse(err error, op pb.CacheRequest_Operation, item *pb.CacheItem) (*pb.CacheResponse, error) {
	if err != nil {
		return nil, cacheError(err, op, item.Key)
	}
	response := &pb.CacheResponse{
		Item: item,
	}
	return response, err
}

func (s *CacheServer) Call(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {

	var err error
	s.c.Lock()
	defer s.c.Unlock()

	switch in.Operation {
	case pb.CacheRequest_SET:
		s.c.Set(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(nil, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_CAS:
		err = s.c.Cas(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second, uint64(in.Item.Cas))
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_GET:
		value, err := s.c.Get(in.Item.Key)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key, Value: value})
	case pb.CacheRequest_GETS:
		value, cas, err := s.c.Gets(in.Item.Key)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key, Value: value, Cas: cas})
	case pb.CacheRequest_ADD:
		err = s.c.Add(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_REPLACE:
		err = s.c.Replace(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_DELETE:
		s.c.Delete(in.Item.Key)
		return cacheResponse(nil, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_TOUCH:
		err = s.c.Touch(in.Item.Key, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_APPEND:
		err = s.c.Append(in.Item.Key, in.Append, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_PREPEND:
		err = s.c.Prepend(in.Item.Key, in.Prepend, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_INCREMENT:
		err = s.c.Increment(in.Item.Key, in.Increment)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_DECREMENT:
		err = s.c.Decrement(in.Item.Key, in.Decrement)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_FLUSHALL:
		s.c.FlushAll()
		return cacheResponse(nil, in.Operation, nil)
	default:
		return nil, status.Errorf(codes.Unimplemented, "unrecognized cache command")
	}

}

// Set set a key/value pair to the cache.
func (s *CacheServer) Set(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_SET
	return s.Call(ctx, in)
}

func (s *CacheServer) Cas(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_CAS
	return s.Call(ctx, in)
}

// Get gets a key/value pair from the cache.
func (s *CacheServer) Get(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_GET
	return s.Call(ctx, in)
}

func (s *CacheServer) Gets(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_GETS
	return s.Call(ctx, in)
}

// Add adds a key/value pair to the cache.
func (s *CacheServer) Add(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_ADD
	return s.Call(ctx, in)
}

// Replace replaces a key/value pair in the cache.
func (s *CacheServer) Replace(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_REPLACE
	return s.Call(ctx, in)
}

func (s *CacheServer) Delete(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_DELETE
	return s.Call(ctx, in)
}

// Touch updates the key/value pair's cas and ttl.
func (s *CacheServer) Touch(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_TOUCH
	return s.Call(ctx, in)
}

func (s *CacheServer) Append(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_APPEND
	return s.Call(ctx, in)
}

func (s *CacheServer) Prepend(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_PREPEND
	return s.Call(ctx, in)
}

func (s *CacheServer) Increment(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_INCREMENT
	return s.Call(ctx, in)
}

func (s *CacheServer) Decrement(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_DECREMENT
	return s.Call(ctx, in)
}

func (s *CacheServer) FlushAll(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_FLUSHALL
	return s.Call(ctx, in)
}
