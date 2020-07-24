package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s cnrService) List(ctx context.Context, req *container.ListRequest) (*container.ListResponse, error) {
	// check healthiness
	if err := s.healthy.Healthy(); err != nil {
		return nil, errors.Wrap(err, "try again later")
	}

	// verify request structure
	if err := requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// list container identifiers from storage
	ownerID := req.GetOwnerID()
	cidList, err := s.cnrStore.List(&ownerID)
	if err != nil {
		return nil, status.Error(
			codes.NotFound,
			errors.Wrap(err, "could not list the containers in storage").Error(),
		)
	}

	// fill the response
	res := new(container.ListResponse)
	res.CID = cidList

	return res, nil
}
