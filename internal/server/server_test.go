package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/mrshabel/gumlog/api/v1"
	"github.com/mrshabel/gumlog/internal/auth"
	"github.com/mrshabel/gumlog/internal/config"
	"github.com/mrshabel/gumlog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	// run a table of tests against the different implementations of the client and server
	table := map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized client fails":                          testUnauthorized,
	}

	for scenario, fn := range table {
		t.Run(scenario, func(t *testing.T) {
			// setup a fresh testing environment with clean state
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			// run test case. only authorized clients will be used for now
			fn(t, rootClient, nobodyClient, config)
		})
	}

}

// a helper function to create an insecure connection to the grpc server on any random port. The listening grpc server is run in a separate goroutine to avoid blocking the main goroutine
func setupTest(t *testing.T, fn func(*Config)) (rootClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	// 0 picks up any arbitrary port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// helper function to create new grpc client with credentials and different permission levels
	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)

		// obtain the client credentials
		clientCreds := credentials.NewTLS(tlsConfig)
		clientOpts := grpc.WithTransportCredentials(clientCreds)
		clientConn, err := grpc.NewClient(l.Addr().String(), clientOpts)
		require.NoError(t, err)

		client := api.NewLogClient(clientConn)
		return clientConn, client, clientOpts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	// configure server tls
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	// temporal directory to store the log files
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// create new instance of the log
	clientLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// add ACL authorizer
	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{CommitLog: clientLog, Authorizer: authorizer}
	// execute the test function with the log configuration
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// expose and serve grpc server in the background
	go func() {
		server.Serve(l)
	}()

	// a helper function to close the connections and cleanup resources
	teardown = func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		// shutdown tcp server listener
		l.Close()
		// remove log
		clientLog.Remove()
	}

	return rootClient, nobodyClient, cfg, teardown
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	// send new record down the wire to the server and consume the response
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	// assert that the received record matches the original record
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

// test that the server returns an error when a record's offset exceeds the highest offset of the log
func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	// consume from a higher offset
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	// cast error as a grpc error and assert that it is an out of range error
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

// stream records between client and server
func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		&api.Record{
			Value:  []byte("first message"),
			Offset: 0,
		},
		&api.Record{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	// stream records to server
	stream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	for offset, record := range records {
		err = stream.Send(&api.ProduceRequest{Record: record})
		require.NoError(t, err)
		res, err := stream.Recv()
		require.NoError(t, err)
		if res.Offset != uint64(offset) {
			t.Fatalf("got offset %d, want: %d", res.Offset, offset)
		}
	}
	// consume stream
	cStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)

	for i, record := range records {
		// receive stream and check that it matches current record
		res, err := cStream.Recv()
		require.NoError(t, err)
		require.Equal(t, res.Record, &api.Record{
			Value:  record.Value,
			Offset: uint64(i),
		})
	}
}

// connect to the server as an authorized client
func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if produce != nil {
		t.Fatal("produce response should be nil")
	}

	// expect permission denied error
	got, want := status.Code(err), codes.PermissionDenied
	require.Equal(t, want, got)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatal("consume response should be nil")
	}

	// expect permission denied error
	got, want = status.Code(err), codes.PermissionDenied
	require.Equal(t, want, got)
}
