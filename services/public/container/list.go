package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
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

	// list containers
	p := libcnr.ListParams{}
	p.SetContext(ctx)
	p.SetOwnerIDList(req.GetOwnerID())

	lRes, err := s.cnrStore.ListContainers(p)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// fill the response
	res := new(container.ListResponse)
	res.CID = lRes.CIDList()

	return res, nil
}
