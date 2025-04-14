package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	// compose status
	st := status.New(
		codes.NotFound, fmt.Sprintf("offset out of range: %d", e.Offset),
	)
	// compose message
	msg := fmt.Sprintf(
		"The requested offset is outside the log's range: %d",
		e.Offset)

	details := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	// compose status with details
	std, err := st.WithDetails(details)
	if err != nil {
		return st
	}
	return std
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
