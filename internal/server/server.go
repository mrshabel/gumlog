package server

import (
	"context"
	"fmt"
	"time"

	api "github.com/mrshabel/gumlog/api/v1"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// a log interface that can be implemented as either an in-memory or persistent log
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
	// authorization enforcer with acl rules
	Authorizer Authorizer
}

// access control constants
const (
	objectWildCard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

// unique context key
type subjectContextKey struct{}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// grpc server stub implementation
var _ api.LogServer = (*grpcServer)(nil)

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	// create a global named logger for the server with configurations
	logger := zap.L().Named("server")
	// record duration of request in log
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns", duration.Nanoseconds(),
				)
			},
		),
	}
	// setup opencensus tracing to sample all traces
	trace.ApplyConfig(trace.Config{
		DefaultSampler: trace.AlwaysSample(),
	})
	if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
		return nil, err
	}
	// trace only 'produce' requests. this will record request/response size, latency
	// halfSampler := trace.ProbabilitySampler(0.5)
	// trace.ApplyConfig(trace.Config{
	// 	DefaultSampler: func(sp trace.SamplingParameters) trace.SamplingDecision {
	// 		if strings.Contains(sp.Name, "Produce") {
	// 			return trace.SamplingDecision{Sample: true}
	// 		}
	// 		return halfSampler(sp)
	// 	},
	// })

	// hook unary and streaming interceptor/middleware into the grpc request
	// the authentication interceptor is registered on the middleware chain
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			// record traces and logs
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_zap.StreamServerInterceptor(logger, zapOpts...),
			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_ctxtags.UnaryServerInterceptor(),
		grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
		grpc_auth.UnaryServerInterceptor(authenticate),
	)))
	// attach opencensus stat handler to record stats
	opts = append(opts, grpc.StatsHandler(&ocgrpc.ServerHandler{}))

	// create a new grpc server and register the service with telemetry options
	gsrv := grpc.NewServer(opts...)
	srv, err := newGRPCServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func newGRPCServer(config *Config) (srv *grpcServer, err error) {
	return &grpcServer{Config: config}, nil
}

// server handlers

// add a new record to the commit log
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// permit only allowed clients
	if err := s.Authorizer.Authorize(subject(ctx), objectWildCard, produceAction); err != nil {
		return nil, err
	}

	// append the record to the log
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

// retrieve a record from the commit log
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	// permit only allowed clients
	if err := s.Authorizer.Authorize(subject(ctx), objectWildCard, consumeAction); err != nil {
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: record}, nil
}

// streaming logs

// bidirectional streaming for clients to send data stream into the server's
// log with live responses
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		// add record to log and stream response to client
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// stream data to client from current offset until the last offset
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		// wait on done channel
		case <-stream.Context().Done():
			return nil
		default:
			// consume client request
			res, err := s.Consume(stream.Context(), req)

			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			// send response to client
			if err = stream.Send(res); err != nil {
				return err
			}
			// proceed to next offset
			req.Offset++
		}
	}
}

// read the subject information of a connected client certificate and write it to the server context using an interceptor(middleware)
func authenticate(ctx context.Context) (context.Context, error) {
	// get the peer information from the given context
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown, "couldn't get peer info",
		).Err()
	}
	// extract the authentication information
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	// cast peer info as tls credential and extract subject common name as specified in the CA certificate
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

// extract the subject information from a given context tree
func subject(ctx context.Context) string {
	fmt.Println("context val:", ctx.Value(subjectContextKey{}))
	return ctx.Value(subjectContextKey{}).(string)
}
