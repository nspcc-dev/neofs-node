package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TreeService represents a tree service instance.
type TreeService interface {
	Synchronize(ctx context.Context, cnr cid.ID, treeID string) error
}

func (s *Server) SynchronizeTree(ctx context.Context, req *control.SynchronizeTreeRequest) (*control.SynchronizeTreeResponse, error) {
	err := s.isValidRequest(req)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// check availability
	err = s.ready()
	if err != nil {
		return nil, err
	}

	if s.treeService == nil {
		return nil, status.Error(codes.Internal, "tree service is disabled")
	}

	b := req.GetBody()

	var cnr cid.ID
	if err := cnr.Decode(b.GetContainerId()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = s.treeService.Synchronize(ctx, cnr, b.GetTreeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := new(control.SynchronizeTreeResponse)
	resp.SetBody(new(control.SynchronizeTreeResponse_Body))

	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
