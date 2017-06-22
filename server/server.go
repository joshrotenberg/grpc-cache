package server

import (
	"log"
	"net"
	"time"

	pb "github.com/joshrotenberg/grpc-cache/cache"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/joshrotenberg/grpc-cache/lru"
)

// CacheServer encapsulates the cache server things.
type CacheServer struct {
	cache      *lru.Cache
	grpcServer *grpc.Server
	listener   net.Listener
}

// NewWithListener returns a new instance of the server given an initialized listener and
// maxEntries for the cache.
func NewWithListener(listener net.Listener, maxEntries int) *CacheServer {
	grpcServer := grpc.NewServer()
	server := CacheServer{
		cache:      lru.New(maxEntries),
		grpcServer: grpcServer,
		listener:   listener,
	}

	pb.RegisterCacheServer(grpcServer, &server)
	return &server
}

// New returns a new instance of the server. It takes host:port and maxEntries arguments,
// which defines the max number of entries allowed in the cache. Set to 0 for unlimited.
func New(address string, maxEntries int) (*CacheServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return NewWithListener(listener, maxEntries), nil
}

// Start starts the cache server. It takes a host:port string on which to run the server.
func (s *CacheServer) Start() {
	go func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
}

// Stop tries to gracefull stop the server.
func (s *CacheServer) Stop() {
	s.grpcServer.GracefulStop()
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

// Call calls the cache operation in in.Operation
func (s *CacheServer) Call(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {

	var err error
	s.cache.Lock()
	defer s.cache.Unlock()

	switch in.Operation {
	case pb.CacheRequest_SET:
		s.cache.Set(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(nil, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_CAS:
		err = s.cache.Cas(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second, uint64(in.Item.Cas))
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_GET:
		value, err := s.cache.Get(in.Item.Key)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key, Value: value})
	case pb.CacheRequest_GETS:
		value, cas, err := s.cache.Gets(in.Item.Key)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key, Value: value, Cas: cas})
	case pb.CacheRequest_ADD:
		err = s.cache.Add(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_REPLACE:
		err = s.cache.Replace(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_DELETE:
		s.cache.Delete(in.Item.Key)
		return cacheResponse(nil, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_TOUCH:
		err = s.cache.Touch(in.Item.Key, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_APPEND:
		err = s.cache.Append(in.Item.Key, in.Append, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_PREPEND:
		err = s.cache.Prepend(in.Item.Key, in.Prepend, time.Duration(in.Item.Ttl)*time.Second)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_INCREMENT:
		err = s.cache.Increment(in.Item.Key, in.Increment)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_DECREMENT:
		err = s.cache.Decrement(in.Item.Key, in.Decrement)
		return cacheResponse(err, in.Operation, &pb.CacheItem{Key: in.Item.Key})
	case pb.CacheRequest_FLUSHALL:
		s.cache.FlushAll()
		return cacheResponse(nil, in.Operation, nil)
	default:
		return nil, status.Errorf(codes.Unimplemented, "unrecognized cache command %d", in.Operation)
	}

}

// Set stores a key/value pair in the cache.
func (s *CacheServer) Set(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_SET
	return s.Call(ctx, in)
}

// Cas does a "check and set" or "compare and swap" operation. If the given key exists
// and the currently stored CAS value matches the one supplied, the value is stored.
func (s *CacheServer) Cas(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_CAS
	return s.Call(ctx, in)
}

// Get gets a key/value pair from the cache.
func (s *CacheServer) Get(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_GET
	return s.Call(ctx, in)
}

// Gets gets a key/value pair from the cache and includes its CAS value.
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

// Delete deletes a key/value pair from the cache.
func (s *CacheServer) Delete(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_DELETE
	return s.Call(ctx, in)
}

// Touch updates the key/value pair's cas and ttl.
func (s *CacheServer) Touch(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_TOUCH
	return s.Call(ctx, in)
}

// Append appends the value to the current value for the key.
func (s *CacheServer) Append(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_APPEND
	return s.Call(ctx, in)
}

// Prepend prepends the value to the current value for the key.
func (s *CacheServer) Prepend(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_PREPEND
	return s.Call(ctx, in)
}

// Increment increments the value for key.
func (s *CacheServer) Increment(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_INCREMENT
	return s.Call(ctx, in)
}

// Decrement decrements the value for the given key.
func (s *CacheServer) Decrement(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_DECREMENT
	return s.Call(ctx, in)
}

// FlushAll deletes all key/value pairs from the cache.
func (s *CacheServer) FlushAll(ctx context.Context, in *pb.CacheRequest) (*pb.CacheResponse, error) {
	in.Operation = pb.CacheRequest_FLUSHALL
	return s.Call(ctx, in)
}
