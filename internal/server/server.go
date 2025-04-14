package server

import (
	"context"

	api "github.com/mrshabel/gumlog/api/v1"
	"google.golang.org/grpc"
)

// a log interface that can be implemented as either an in-memory or persistent log
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

// grpc server stub implementation
var _ api.LogServer = (*grpcServer)(nil)

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	// create a new grpc server and register the service
	gsrv := grpc.NewServer()
	srv, err := newGRPCServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGRPCServer(config *Config) (srv *grpcServer, err error) {
	return &grpcServer{Config: config}, nil
}

// server handlers

// add a new record to the commit log
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// append the record to the log
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

// retrieve a record from the commit log
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
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
