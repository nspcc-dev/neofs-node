package control

import (
	"errors"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
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

	var (
		cursor    *engine.Cursor
		addresses []objectcore.AddressWithAttributes
	)
	// (Limit 4MB - 64KB for service bytes and future fields) / 89B address length = 46390 addresses can be sent
	const count = 46390
	for {
		addresses, cursor, err = s.storage.ListWithCursor(count, cursor)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				return nil
			}

			return status.Error(codes.Internal, err.Error())
		}

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
	}
}
