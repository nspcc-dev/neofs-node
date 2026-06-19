package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConvertContextStatus checks whether error is a context-related gRPC status.
// If so, corresponding context error is returned. Otherwise, err is returned.
func ConvertContextStatus(err error) error {
	// track https://github.com/nspcc-dev/neofs-sdk-go/issues/624
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	default:
		return err
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	}
}

// IsUnavailable checks whether err corresponds to UNAVAILABLE gRPC status.
func IsUnavailable(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}
