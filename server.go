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

func cacheError(err error, op string, key string) error {
	switch err {
	case lru.ErrNotFound:
		return status.Errorf(codes.NotFound, "%s error: '%s' not found", op, key)
	case lru.ErrExists:
		return status.Errorf(codes.AlreadyExists, "%s error: '%s' exists", op, key)
	}
	return err
}

// Set set a key/value pair to the cache.
func (s *CacheServer) Set(ctx context.Context, in *pb.SetRequest) (*pb.SetReply,
	error) {

	s.c.Lock()
	s.c.Set(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
	s.c.Unlock()

	reply := &pb.SetReply{
		Item: &pb.CacheItem{Key: in.Item.Key},
	}
	return reply, nil
}

// Get gets a key/value pair from the cache.
func (s *CacheServer) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetReply,
	error) {

	s.c.Lock()
	value, err := s.c.Get(in.Item.Key)
	s.c.Unlock()

	if err != nil {
		return nil, cacheError(err, "get", in.Item.Key)
	}

	reply := &pb.GetReply{
		Item: &pb.CacheItem{Key: in.Item.Key, Value: value},
	}
	return reply, nil
}

// Add adds a key/value pair to the cache.
func (s *CacheServer) Add(ctx context.Context, in *pb.AddRequest) (*pb.AddReply,
	error) {

	s.c.Lock()
	err := s.c.Add(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
	s.c.Unlock()

	if err != nil {
		return nil, cacheError(err, "add", in.Item.Key)
	}

	reply := &pb.AddReply{
		Item: &pb.CacheItem{Key: in.Item.Key},
	}
	return reply, nil
}

// Replace replaces a key/value pair in the cache.
func (s *CacheServer) Replace(ctx context.Context, in *pb.ReplaceRequest) (*pb.ReplaceReply,
	error) {

	s.c.Lock()
	err := s.c.Replace(in.Item.Key, in.Item.Value, time.Duration(in.Item.Ttl)*time.Second)
	s.c.Unlock()

	if err != nil {
		return nil, cacheError(err, "replace", in.Item.Key)
	}

	reply := &pb.ReplaceReply{
		Item: &pb.CacheItem{Key: in.Item.Key},
	}
	return reply, nil
}

// Touch updates the key/value pair's cas and ttl.
func (s *CacheServer) Touch(ctx context.Context, in *pb.TouchRequest) (*pb.TouchReply,
	error) {

	s.c.Lock()
	err := s.c.Touch(in.Item.Key, time.Duration(in.Item.Ttl)*time.Second)
	s.c.Unlock()

	if err != nil {
		return nil, cacheError(err, "touch", in.Item.Key)
	}

	reply := &pb.TouchReply{
		Item: &pb.CacheItem{Key: in.Item.Key},
	}
	return reply, nil
}
