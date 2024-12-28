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

// NotaryList returns list of transactions dumbs of the IR notary requests.
//
// If request is not signed with a key from white list, permission error returns.
func (s *Server) NotaryList(_ context.Context, req *control.NotaryListRequest) (*control.NotaryListResponse, error) {
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	resp := new(control.NotaryListResponse)

	body := new(control.NotaryListResponse_Body)
	resp.SetBody(body)

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
	body.SetTransactions(txsInfo)

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

	resp := new(control.NotaryRequestResponse)

	body := new(control.NotaryRequestResponse_Body)
	resp.SetBody(body)

	hash, err := s.prm.notaryManager.RequestNotary(req.GetBody().GetMethod(), req.GetBody().Args...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	body.SetHash(hash.BytesBE())

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

	resp := new(control.NotarySignResponse)

	body := new(control.NotarySignResponse_Body)
	resp.SetBody(body)

	hash, err := util.Uint256DecodeBytesBE(req.GetBody().GetHash())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	err = s.prm.notaryManager.SignNotary(hash)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := SignMessage(&s.prm.key.PrivateKey, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
