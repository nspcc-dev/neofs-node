package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
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

	p := libcnr.DeleteParams{}
	p.SetContext(ctx)
	p.SetCID(req.GetCID())
	// TODO: add owner ID and CID signature

	if _, err := s.cnrStore.DeleteContainer(p); err != nil {
		return nil, status.Error(
			codes.Aborted,
			errors.Wrapf(err, "could not remove container %d", req.CID).Error(),
		)
	}

	return new(container.DeleteResponse), nil
}
