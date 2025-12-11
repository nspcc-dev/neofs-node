package control

import (
	"context"

	"github.com/nspcc-dev/neo-go/pkg/util"
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
	var resp = &control.HealthCheckResponse{
		Body: &control.HealthCheckResponse_Body{
			HealthStatus: s.prm.healthChecker.HealthStatus(),
		},
	}

	// sign the response
	if err := SignMessage(&s.prm.key.PrivateKey, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// NotaryList returns list of transactions dumbs of the IR notary requests.
//
// If request is not signed with a key from white list, permission error returns.
func (s *Server) NotaryList(_ context.Context, req *control.NotaryListRequest) (*control.NotaryListResponse, error) {
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	txs, err := s.prm.notaryManager.ListNotaryRequests()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	txsInfo := make([]*control.TransactionInfo, 0, len(txs))
	for _, tx := range txs {
		txsInfo = append(txsInfo, &control.TransactionInfo{
			Hash: tx.BytesBE(),
		})
	}
	var resp = &control.NotaryListResponse{
		Body: &control.NotaryListResponse_Body{
			Transactions: txsInfo,
		},
	}

	if err := SignMessage(&s.prm.key.PrivateKey, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// NotaryRequest create and send notary request and returns a hash of this request.
//
// If request is not signed with a key from white list, permission error returns.
func (s *Server) NotaryRequest(_ context.Context, req *control.NotaryRequestRequest) (*control.NotaryRequestResponse, error) {
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	hash, err := s.prm.notaryManager.RequestNotary(req.GetBody().GetMethod(), req.GetBody().Args...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var resp = &control.NotaryRequestResponse{
		Body: &control.NotaryRequestResponse_Body{
			Hash: hash.BytesBE(),
		},
	}

	if err := SignMessage(&s.prm.key.PrivateKey, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// NotarySign send notary request to forceful container removal and returns a hash of this request.
//
// If request is not signed with a key from white list, permission error returns.
func (s *Server) NotarySign(_ context.Context, req *control.NotarySignRequest) (*control.NotarySignResponse, error) {
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	hash, err := util.Uint256DecodeBytesBE(req.GetBody().GetHash())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	err = s.prm.notaryManager.SignNotary(hash)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var resp = &control.NotarySignResponse{Body: new(control.NotarySignResponse_Body)}

	if err := SignMessage(&s.prm.key.PrivateKey, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
