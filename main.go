package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joshrotenberg/grpc-cache/server"
)

var (
	serverAddr      string
	cacheMaxEntries int
)

func init() {
	flag.StringVar(&serverAddr, "addr", "", "host:port to listen on")
	flag.IntVar(&cacheMaxEntries, "maxEntries", 0, "maxiumum cache entries")
	flag.Parse()
}
func main() {
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	s, err := server.New(serverAddr, cacheMaxEntries)
	if err != nil {
		log.Fatal(err)
	}
	s.Start()
	<-sigs
	s.Stop()
}
