package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/container"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
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

	// get container
	p := libcnr.GetParams{}
	p.SetContext(ctx)
	p.SetCID(req.GetCID())

	gRes, err := s.cnrStore.GetContainer(p)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// fill the response
	res := new(container.GetResponse)
	res.Container = gRes.Container()

	return res, nil
}
