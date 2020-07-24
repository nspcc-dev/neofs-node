package container

import (
	"context"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s cnrService) Get(ctx context.Context, req *container.GetRequest) (*container.GetResponse, error) {
	// check healthiness
	if err := s.healthy.Healthy(); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	// verify request structure
	if err := requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// get container from storage
	cnr, err := s.cnrStore.Get(req.GetCID())
	if err != nil {
		return nil, status.Error(
			codes.NotFound,
			errors.Wrap(err, "could not get container from storage").Error(),
		)
	}

	// fill the response
	res := new(container.GetResponse)

	// FIXME: salt should be []byte in the message
	salt, err := uuid.FromBytes(cnr.Salt())
	if err != nil {
		return nil, status.Error(
			codes.Aborted,
			errors.Wrap(err, "could not decode salt").Error(),
		)
	}

	// FIXME: message field should be the same type or []byte.
	res.Container = new(container.Container)
	res.Container.Salt = refs.UUID(salt)
	res.Container = new(container.Container)
	res.Container.OwnerID = cnr.OwnerID()
	res.Container.Rules = cnr.PlacementRule()
	res.Container.BasicACL = basic.ToUint32(cnr.BasicACL())

	return res, nil
}
