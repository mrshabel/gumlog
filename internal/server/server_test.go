package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/mrshabel/gumlog/api/v1"
	"github.com/mrshabel/gumlog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	// run a table of tests against the different implementations of the client and server
	table := map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	}

	for scenario, fn := range table {
		t.Run(scenario, func(t *testing.T) {
			// setup a fresh testing environment with clean state
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}

}

// a helper function to create an insecure connection to the grpc server on any random port. The listening grpc server is run in a separate goroutine to avoid blocking the main goroutine
func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	// 0 picks up any arbitrary port
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// setup an insecure connection
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	clientConn, err := grpc.NewClient(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	// temporal directory to store the log files
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	// create new instance of the log
	clientLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{CommitLog: clientLog}
	// execute the test function with the log configuration
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	// expose and serve grpc server in the background
	go func() {
		server.Serve(l)
	}()
	// create a new grpc log client
	client = api.NewLogClient(clientConn)
	// a helper function to close the connections and cleanup resources
	teardown = func() {
		server.Stop()
		clientConn.Close()
		// shutdown tcp server listener
		l.Close()
		// remove log
		clientLog.Remove()
	}

	return client, cfg, teardown
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
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
func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
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
func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
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
