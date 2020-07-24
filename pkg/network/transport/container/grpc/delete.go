package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s cnrService) Delete(ctx context.Context, req *container.DeleteRequest) (*container.DeleteResponse, error) {
	// check healthiness
	if err := s.healthy.Healthy(); err != nil {
		return nil, errors.Wrap(err, "try again later")
	}

	// verify request structure
	if err := requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// remove container from storage
	if err := s.cnrStore.Delete(req.GetCID()); err != nil {
		return nil, status.Error(
			codes.Aborted,
			errors.Wrap(err, "could not remove container from storage").Error(),
		)
	}

	return new(container.DeleteResponse), nil
}
