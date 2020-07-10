package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/refs"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
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
	cnr := new(container.Container)
	cnr.OwnerID = req.GetOwnerID()
	cnr.Capacity = req.GetCapacity()
	cnr.Rules = req.GetRules()
	cnr.BasicACL = req.GetBasicACL()

	var err error
	if cnr.Salt, err = refs.NewUUID(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// put the container to storage
	p := libcnr.PutParams{}
	p.SetContext(ctx)
	p.SetContainer(cnr)
	// TODO: add user signature

	pRes, err := s.cnrStore.PutContainer(p)
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	// fill the response
	res := new(container.PutResponse)
	res.CID = pRes.CID()

	return res, nil
}
