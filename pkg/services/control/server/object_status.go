package control

import (
	"context"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ObjectStatus(_ context.Context, request *control.ObjectStatusRequest) (*control.ObjectStatusResponse, error) {
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
	err = addr.DecodeString(request.GetBody().GetObjectAddress())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing object address: %s", err)
	}

	st, err := s.storage.ObjectStatus(addr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "storage engine error: %s", err)
	}

	resp := &control.ObjectStatusResponse{
		Body: &control.ObjectStatusResponse_Body{},
	}

	for _, sh := range st.Shards {
		respSh := new(control.ObjectStatusResponse_Body_Shard)
		respSh.ShardId = sh.ID

		if len(sh.Shard.Metabase.State) == 0 {
			// can be reconsidered since it is possible to get
			// resynchronized state when meta knows nothing about
			// stored objects in blob; however, it is a control
			// service, not a debug util
			continue
		}

		respSh.Storages = append(respSh.Storages, &control.ObjectStatusResponse_Body_Shard_Status{
			Type:   "metabase",
			Status: strings.Join(sh.Shard.Metabase.State, ","),
		})

		for _, subStorage := range sh.Shard.Blob.Substorages {
			respSh.Storages = append(respSh.Storages,
				&control.ObjectStatusResponse_Body_Shard_Status{
					Type:   subStorage.Type,
					Status: fmt.Sprintf("path: %q", subStorage.Path),
				},
			)
		}

		var wcStatus string
		if sh.Shard.Writecache.PathFSTree != "" {
			wcStatus = fmt.Sprintf("fsTree path: %q", sh.Shard.Writecache.PathFSTree)
		}

		// it can be turned off, it is OK
		if wcStatus != "" {
			respSh.Storages = append(respSh.Storages,
				&control.ObjectStatusResponse_Body_Shard_Status{
					Type:   "write-cache",
					Status: wcStatus,
				},
			)
		}

		resp.Body.Shards = append(resp.Body.Shards, respSh)
	}

	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
