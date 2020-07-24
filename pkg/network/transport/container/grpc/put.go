package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO verify MessageID.
func (s cnrService) Put(ctx context.Context, req *container.PutRequest) (*container.PutResponse, error) {
	// check healthiness
	if err := s.healthy.Healthy(); err != nil {
		return nil, errors.Wrap(err, "try again later")
	}

	// verify request structure
	if err := requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// create container structure
	// FIXME: message field should be the same type or []byte.
	cnr := new(Container)
	cnr.SetOwnerID(req.GetOwnerID())
	cnr.SetPlacementRule(req.GetRules())
	cnr.SetBasicACL(basic.FromUint32(req.GetBasicACL()))

	uid, err := refs.NewUUID()
	if err != nil {
		return nil, status.Error(
			codes.Aborted,
			errors.Wrap(err, "could not generate the salt").Error(),
		)
	}

	cnr.SetSalt(uid.Bytes())

	// save container in storage
	cid, err := s.cnrStore.Put(cnr)
	if err != nil {
		return nil, status.Error(
			codes.Aborted,
			errors.Wrap(err, "could not save the container instorage").Error(),
		)
	}

	// fill the response
	res := new(container.PutResponse)
	res.CID = *cid

	return res, nil
}
