package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ReviveObject(_ context.Context, request *control.ReviveObjectRequest) (*control.ReviveObjectResponse, error) {
	err := s.isValidRequest(request)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// check availability
	err = s.ready()
	if err != nil {
		return nil, err
	}

	var addr oid.Address
	err = addr.DecodeString(string(request.GetBody().GetObjectAddress()))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing object address: %s", err)
	}

	res, err := s.storage.ReviveObject(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &control.ReviveObjectResponse{
		Body: &control.ReviveObjectResponse_Body{},
	}
	for _, sh := range res.Shards {
		respSh := new(control.ReviveObjectResponse_Body_Shard)
		respSh.ShardId = sh.ID
		respSh.Status = sh.Status.Message()

		resp.Body.Shards = append(resp.Body.Shards, respSh)
	}

	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
