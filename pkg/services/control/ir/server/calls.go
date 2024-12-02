package control

import (
	"context"

	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HealthCheck returns health status of the local IR node.
//
// If request is not signed with a key from white list, permission error returns.
func (s *Server) HealthCheck(_ context.Context, req *control.HealthCheckRequest) (*control.HealthCheckResponse, error) {
	// verify request
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// create and fill response
	resp := new(control.HealthCheckResponse)

	body := new(control.HealthCheckResponse_Body)
	resp.SetBody(body)

	body.SetHealthStatus(s.prm.healthChecker.HealthStatus())

	// sign the response
	if err := SignMessage(&s.prm.key.PrivateKey, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// NetworkList returns list of hashes of the IR notary requests.
//
// If request is not signed with a key from white list, permission error returns.
func (s *Server) NetworkList(_ context.Context, req *control.NetworkListRequest) (*control.NetworkListResponse, error) {
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	resp := new(control.NetworkListResponse)

	body := new(control.NetworkListResponse_Body)
	resp.SetBody(body)

	body.SetHashes(s.prm.networkManager.ListNetworkRequests())

	if err := SignMessage(&s.prm.key.PrivateKey, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// NetworkEpochTick send notary request to tick epoch and returns a hash of this request.
//
// If request is not signed with a key from white list, permission error returns.
func (s *Server) NetworkEpochTick(_ context.Context, req *control.NetworkEpochTickRequest) (*control.NetworkEpochTickResponse, error) {
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	resp := new(control.NetworkEpochTickResponse)

	body := new(control.NetworkEpochTickResponse_Body)
	resp.SetBody(body)

	body.SetHash(s.prm.networkManager.TickEpoch())

	if err := SignMessage(&s.prm.key.PrivateKey, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
