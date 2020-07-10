package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-node/lib/acl"
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

	// store binary EACL
	key := acl.BinaryEACLKey{}
	key.SetCID(req.GetID())

	val := acl.BinaryEACLValue{}
	val.SetEACL(req.GetEACL())
	val.SetSignature(req.GetSignature())

	if err := s.aclStore.PutBinaryEACL(ctx, key, val); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
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

	// receive binary EACL
	key := acl.BinaryEACLKey{}
	key.SetCID(req.GetID())

	val, err := s.aclStore.GetBinaryEACL(ctx, key)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// fill the response
	res := new(container.GetExtendedACLResponse)
	res.SetEACL(val.EACL())
	res.SetSignature(val.Signature())

	return res, nil
}
