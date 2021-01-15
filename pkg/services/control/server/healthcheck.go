package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HealthCheck returns health status of the local node.
//
// If request is unsigned or signed by disallowed key, permission error returns.
func (s *Server) HealthCheck(_ context.Context, req *control.HealthCheckRequest) (*control.HealthCheckResponse, error) {
	// verify request
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// create and fill response
	resp := new(control.HealthCheckResponse)

	body := new(control.HealthCheckResponse_Body)
	resp.SetBody(body)

	body.SetNetmapStatus(s.healthChecker.NetmapStatus())

	// sign the response
	if err := SignMessage(s.key, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
