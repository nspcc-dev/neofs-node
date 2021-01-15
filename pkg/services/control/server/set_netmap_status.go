package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SetNetmapStatus sets node status in NeoFS network.
//
// If request is unsigned or signed by disallowed key, permission error returns.
func (s *Server) SetNetmapStatus(ctx context.Context, req *control.SetNetmapStatusRequest) (*control.SetNetmapStatusResponse, error) {
	// verify request
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// set node status
	if err := s.nodeState.SetNetmapStatus(req.GetBody().GetStatus()); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	// create and fill response
	resp := new(control.SetNetmapStatusResponse)

	body := new(control.SetNetmapStatusResponse_Body)
	resp.SetBody(body)

	// sign the response
	if err := SignMessage(s.key, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
