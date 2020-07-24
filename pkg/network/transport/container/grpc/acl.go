package container

import (
	"context"

	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s cnrService) SetExtendedACL(ctx context.Context, req *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	// check healthiness
	if err := s.healthy.Healthy(); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	// verify request structure
	if err := requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// unmarshal eACL table
	table, err := eacl.UnmarshalTable(req.GetEACL())
	if err != nil {
		return nil, status.Error(
			codes.InvalidArgument,
			errors.Wrap(err, "could not decode eACL table").Error(),
		)
	}

	// store eACL table
	if err := s.aclStore.PutEACL(req.GetID(), table, req.GetSignature()); err != nil {
		return nil, status.Error(
			codes.Aborted,
			errors.Wrap(err, "could not save eACL in storage").Error(),
		)
	}

	return new(container.SetExtendedACLResponse), nil
}

func (s cnrService) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	// check healthiness
	if err := s.healthy.Healthy(); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	// verify request structure
	if err := requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// receive binary eACL
	table, err := s.aclStore.GetEACL(req.GetID())
	if err != nil {
		return nil, status.Error(
			codes.NotFound,
			errors.Wrap(err, "could not get eACL from storage").Error(),
		)
	}

	// fill the response
	res := new(container.GetExtendedACLResponse)
	res.SetEACL(eacl.MarshalTable(table))
	res.SetSignature(nil) // TODO: set signature when will appear.

	return res, nil
}
