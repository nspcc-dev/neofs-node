package accounting

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"

	apiaccounting "github.com/nspcc-dev/neofs-api-go/v2/accounting"
	protoaccounting "github.com/nspcc-dev/neofs-api-go/v2/accounting/grpc"
	apirefs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	protosession "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// BalanceContract groups ops of the Balance contract deployed in the FS chain
// required to serve NeoFS API Accounting service.
type BalanceContract interface {
	// Decimals returns the number of decimals used by NeoFS tokens.
	Decimals() (uint32, error)
	// BalanceOf returns current balance of the referenced user in NeoFS tokens.
	BalanceOf(user.ID) (*big.Int, error)
}

type server struct {
	protoaccounting.UnimplementedAccountingServiceServer
	signer   *ecdsa.PrivateKey
	net      netmap.State
	contract BalanceContract
}

// New provides protoaccounting.AccountingServiceServer based on specified
// [BalanceContract].
//
// All response messages are signed using specified signer and have current
// epoch in the meta header.
func New(s *ecdsa.PrivateKey, net netmap.State, c BalanceContract) protoaccounting.AccountingServiceServer {
	return &server{
		signer:   s,
		net:      net,
		contract: c,
	}
}

func (s *server) makeBalanceResponse(body *protoaccounting.BalanceResponse_Body, st *protostatus.Status) (*protoaccounting.BalanceResponse, error) {
	v := version.Current()
	var v2 apirefs.Version
	v.WriteToV2(&v2)
	resp := &protoaccounting.BalanceResponse{
		Body: body,
		MetaHeader: &protosession.ResponseMetaHeader{
			Version: v2.ToGRPCMessage().(*refs.Version),
			Epoch:   s.net.CurrentEpoch(),
			Status:  st,
		},
	}
	return util.SignResponse(s.signer, resp, apiaccounting.BalanceResponse{}), nil
}

func (s *server) makeFailedBalanceResponse(err error) (*protoaccounting.BalanceResponse, error) {
	return s.makeBalanceResponse(nil, util.ToStatus(err))
}

// Balance gets current balance of the requested user using underlying
// [BalanceContract] and returns result in the response.
func (s *server) Balance(_ context.Context, req *protoaccounting.BalanceRequest) (*protoaccounting.BalanceResponse, error) {
	balReq := new(apiaccounting.BalanceRequest)
	if err := balReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	if err := signature.VerifyServiceMessage(balReq); err != nil {
		return s.makeFailedBalanceResponse(util.ToRequestSignatureVerificationError(err))
	}

	mUsr := req.GetBody().GetOwnerId()
	if mUsr == nil {
		return s.makeFailedBalanceResponse(errors.New("missing account"))
	}
	var id2 apirefs.OwnerID
	if err := id2.FromGRPCMessage(mUsr); err != nil {
		panic(err)
	}
	var id user.ID
	if err := id.ReadFromV2(id2); err != nil {
		return s.makeFailedBalanceResponse(fmt.Errorf("invalid account: %w", err))
	}

	bal, err := s.contract.BalanceOf(id)
	if err != nil {
		return s.makeFailedBalanceResponse(err)
	}
	ds, err := s.contract.Decimals()
	if err != nil {
		return s.makeFailedBalanceResponse(err)
	}

	body := &protoaccounting.BalanceResponse_Body{
		Balance: &protoaccounting.Decimal{Value: bal.Int64(), Precision: ds},
	}
	return s.makeBalanceResponse(body, util.StatusOK)
}
