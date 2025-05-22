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
func (s *Server) SetNetmapStatus(_ context.Context, req *control.SetNetmapStatusRequest) (*control.SetNetmapStatusResponse, error) {
	// verify request
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// check availability
	err := s.ready()
	if err != nil {
		return nil, err
	}

	bodyReq := req.GetBody()
	st := bodyReq.GetStatus()

	err = s.nodeState.SetNetmapStatus(st)
	if err != nil {
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
