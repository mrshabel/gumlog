package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	api "github.com/mrshabel/gumlog/api/v1"
	"github.com/mrshabel/gumlog/internal/agent"
	"github.com/mrshabel/gumlog/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	// setup server tls certs and peer certs
	// server tls config will be sent to clients
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	// peer tls config will be shared between servers for replication purposes
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	// setup cluster of 3 nodes acting as replication agents
	var agents []*agent.Agent
	for i := range 3 {
		// get 2 random ports without listener for testing
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("127.0.0.1:%d", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		// use starting node as an entry point for newly discovered nodes to connect to
		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprint(i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	// cleanup function to verify that agents can gracefully shutdown
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()
	time.Sleep(3 * time.Second)

	dummy := []byte("dummy")
	// leader node for writes
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(context.Background(), &api.ProduceRequest{
		Record: &api.Record{
			Value: dummy,
		},
	})
	require.NoError(t, err)
	consumeResponse, err := leaderClient.Consume(context.Background(), &api.ConsumeRequest{
		Offset: produceResponse.Offset,
	})
	require.NoError(t, err)
	// check that consumed data is the same as produced data
	require.Equal(t, consumeResponse.Record.Value, dummy)

	// wait for replication to eventually complete
	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(context.Background(), &api.ConsumeRequest{
		Offset: produceResponse.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, dummy)
}

// helper function for creating a new grpc client for the log service
func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	// create grpc connection
	conn, err := grpc.NewClient(rpcAddr, opts...)
	require.NoError(t, err)

	return api.NewLogClient(conn)
}
