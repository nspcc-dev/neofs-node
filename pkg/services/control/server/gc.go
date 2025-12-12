package control

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DropObjects marks objects to be removed from the local node.
//
// Objects are marked via garbage collector's callback.
//
// If some address is not a valid object address in a binary format, an error returns.
// If request is unsigned or signed by disallowed key, permission error returns.
func (s *Server) DropObjects(_ context.Context, req *control.DropObjectsRequest) (*control.DropObjectsResponse, error) {
	// verify request
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// check availability
	err := s.ready()
	if err != nil {
		return nil, err
	}

	binAddrList := req.GetBody().GetAddressList()
	addrList := make([]oid.Address, len(binAddrList))

	for i := range binAddrList {
		err := addrList[i].DecodeString(string(binAddrList[i]))
		if err != nil {
			return nil, status.Error(codes.InvalidArgument,
				fmt.Sprintf("invalid binary object address: %v", err),
			)
		}
	}

	var firstErr error
	for i := range addrList {
		err := s.storage.Drop(addrList[i])
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return nil, status.Error(codes.Internal, firstErr.Error())
	}

	// create and fill response
	var resp = &control.DropObjectsResponse{Body: new(control.DropObjectsResponse_Body)}

	// sign the response
	if err := SignMessage(s.key, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
