// this file contains the implementation of a replication component that uses a native pull-replication approach to retrieve data when a server joins or leaves
package log

import (
	"context"
	"sync"

	api "github.com/mrshabel/gumlog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	// grpc connection setup
	DialOptions []grpc.DialOption
	// server api
	LocalServer api.LogClient

	logger *zap.Logger
	mu     sync.Mutex
	// servers is a map of all server addresses to channels that can be used to stop replicating data to that server
	servers map[string]chan struct{}
	// status of the replicator
	closed bool
	// close channel for the replicator
	close chan struct{}
}

// Join adds the server address to the list of servers to start replication
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// initialize replicator
	r.init()

	// stop operation if replicator is closed
	if r.closed {
		return nil
	}
	// skip if server is already replicating
	if _, ok := r.servers[name]; ok {
		return nil
	}

	r.servers[name] = make(chan struct{})

	// begin replication in the background
	go r.replicate(addr, r.servers[name])
	return nil
}

// replicate creates a grpc client and open up a server stream to consume all logs on the server
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	// connect to server
	cc, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial remote leader  server", addr)
		return
	}
	defer cc.Close()

	// create grpc api client
	client := api.NewLogClient(cc)

	// request for record stream from start of the log
	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if err != nil {
		r.logError(err, "failed to consume data from server", addr)
		return
	}

	// store records
	records := make(chan *api.Record)

	// consume stream in the background
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive data from server", addr)
				return
			}

			// write received record to records channel
			records <- recv.Record
		}
	}()

	for {
		select {
		// stop operations when replicator is closed
		case <-r.close:
			return
			// stop operation when remote leader server leaves the replication cluster
		case <-leave:
			return
		// write copy of received record to the local server
		case record := <-records:
			_, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{
				Record: record,
			})
			if err != nil {
				r.logError(err, "failed to produce record to local server", addr)
				return
			}
		}
	}
}

// Leave removes the server from the replication cluster and closes the server's associated channel while signaling the follower receiver in the "replicate" goroutine to stop replicating from that server
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	// stop operation if server does not exist
	if _, ok := r.servers[name]; !ok {
		return nil
	}

	// close current server channel and remove its entry
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// init sets up logger and replicator channels
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close closes the replicator and stops replicating to new and existing servers
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
