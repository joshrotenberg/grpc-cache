package server

import (
	"log"
	"net"
	"sync"

	pb "github.com/joshrotenberg/grpc-cache/cache"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/golang/groupcache/lru"
)

// CacheServer encapsulates the cache server things.
type CacheServer struct {
	c *lru.Cache
	s *grpc.Server
	m *sync.Mutex
}

// NewCacheServer returns a new instance of the server. It takes a maxEntries argument,
// which defines the max number of entries allowed in the cache. Set to 0 for unlimited.
func NewCacheServer(maxEntries int) *CacheServer {

	s := grpc.NewServer()
	c := lru.New(maxEntries)
	m := &sync.Mutex{}
	cs := &CacheServer{c: c, s: s, m: m}
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

// Add adds a key/value pair to the cache.
func (s *CacheServer) Add(ctx context.Context, in *pb.AddRequest) (*pb.AddReply,
	error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.c.Add(in.Key, in.Value)
	return &pb.AddReply{Key: in.Key}, nil
}

// Get gets an item from the cache. Check `Found` to see if the value was availa ble.
func (s *CacheServer) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetReply, error) {
	if v, ok := s.c.Get(in.Key); ok == true {
		return &pb.GetReply{Ok: ok, Key: in.Key, Value: v.([]byte)}, nil
	}
	return &pb.GetReply{Ok: false, Key: in.Key, Value: nil}, nil
}

// Remove removes an item from the cache.
func (s *CacheServer) Remove(ctx context.Context, in *pb.RemoveRequest) (*pb.RemoveReply, error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.c.Remove(in.Key)
	return &pb.RemoveReply{Key: in.Key}, nil
}

// Len returns the current number of items in the cache.
func (s *CacheServer) Len(ctx context.Context, in *pb.LenRequest) (*pb.LenReply, error) {
	l := s.c.Len()
	return &pb.LenReply{Len: int32(l)}, nil
}

// Clear removes all items from the cache.
func (s *CacheServer) Clear(ctx context.Context, in *pb.ClearRequest) (*pb.ClearReply, error) {
	s.c.Clear()
	return &pb.ClearReply{}, nil
}
