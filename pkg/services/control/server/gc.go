package control

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DeletedObjectHandler is a handler of objects to be removed.
type DeletedObjectHandler func([]*object.Address) error

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

	binAddrList := req.GetBody().GetAddressList()
	addrList := make([]*object.Address, 0, len(binAddrList))

	for i := range binAddrList {
		a := object.NewAddress()

		err := a.Unmarshal(binAddrList[i])
		if err != nil {
			return nil, status.Error(codes.InvalidArgument,
				fmt.Sprintf("invalid binary object address: %v", err),
			)
		}

		addrList = append(addrList, a)
	}

	err := s.delObjHandler(addrList)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// create and fill response
	resp := new(control.DropObjectsResponse)

	body := new(control.DropObjectsResponse_Body)
	resp.SetBody(body)

	// sign the response
	if err := SignMessage(s.key, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
