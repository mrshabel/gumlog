package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	api "github.com/mrshabel/gumlog/api/v1"
	"github.com/mrshabel/gumlog/internal/auth"
	"github.com/mrshabel/gumlog/internal/discovery"
	"github.com/mrshabel/gumlog/internal/log"
	"github.com/mrshabel/gumlog/internal/server"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Agent sets up and manages all components and processes for a server to initiate its replication process
type Agent struct {
	Config Config

	// internal components for the log, server, service discovery membership and replicator
	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

// Config contains all the details needed to setup each component in the Agent
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

// RPCAddr returns the RPC address from the binding address and the configured RPC port. A non-nil error is returned if the BindAddr is invalid
func (c *Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// New creates and sets up an agent together with its components as defined in the config argument. Calling New starts up a running, functioning service. The created agent is returned if no error occurs else a non-nil error is returned
func New(config Config) (*Agent, error) {
	agent := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	// set up all components
	setup := []func() error{
		agent.setupLogger,
		agent.setupLog,
		agent.setupServer,
		agent.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return agent, nil
}

func (a *Agent) setupLogger() error {
	// start a new development logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(a.Config.DataDir, log.Config{})
	return err
}

func (a *Agent) setupServer() error {
	// setup server with authorization policies
	authorizer := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	// setup grpc server
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	if a.server, err = server.NewGRPCServer(serverConfig, opts...); err != nil {
		return err
	}
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	// setup grpc server listener in the background
	go func() {
		if err := a.server.Serve(ln); err != nil {
			// shutdown agent on listening failure
			a.Shutdown()
		}
	}()

	return err
}

// setupMembership sets up a Replicator needed to connect to other services and a client for the replicator to connect to other servers and consume their data
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	// setup serf membership grpc client
	var opts []grpc.DialOption
	if a.Config.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(a.Config.PeerTLSConfig)))
	}
	conn, err := grpc.NewClient(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	// create new discovery membership for client
	a.membership, err = discovery.New(a.replicator, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	},
	)
	return err
}

// Shutdown shutdowns an agent and its components once with a mutex
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	// mark agent as down and close all channels
	a.shutdown = true
	close(a.shutdowns)

	stopServer := func() error {
		a.server.GracefulStop()
		return nil
	}
	shutdown := []func() error{
		a.membership.Leave, a.replicator.Close,
		stopServer,
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
