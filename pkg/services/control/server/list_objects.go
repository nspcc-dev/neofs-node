package control

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ListObjects(req *control.ListObjectsRequest, stream control.ControlService_ListObjectsServer) error {
	// verify request
	if err := s.isValidRequest(req); err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}

	// check availability
	err := s.ready()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	var prm engine.ListWithCursorPrm
	// (Limit 4MB - 64KB for service bytes and future fields) / 89B address length = 46390 addresses can be sent
	prm.WithCount(46390)
	for {
		res, err := s.storage.ListWithCursor(prm)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				return nil
			}

			return status.Error(codes.Internal, err.Error())
		}

		addresses := res.AddressList()
		objectsAddresses := make([][]byte, 0, len(addresses))
		for _, objectId := range addresses {
			objectsAddresses = append(objectsAddresses, []byte(objectId.Address.EncodeToString()))
		}

		resp := &control.ListObjectsResponse{
			Body: &control.ListObjectsResponse_Body{
				ObjectAddress: objectsAddresses,
			},
		}

		// sign the response
		if err = SignMessage(s.key, resp); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err = stream.Send(resp); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		prm.WithCursor(res.Cursor())
	}
}
