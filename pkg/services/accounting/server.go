package accounting

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	protoaccounting "github.com/nspcc-dev/neofs-sdk-go/proto/accounting"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
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

func (s *server) makeBalanceResponse(body *protoaccounting.BalanceResponse_Body, st *protostatus.Status, req *protoaccounting.BalanceRequest) (*protoaccounting.BalanceResponse, error) {
	resp := &protoaccounting.BalanceResponse{
		Body: body,
		MetaHeader: &protosession.ResponseMetaHeader{
			Version: version.Current().ProtoMessage(),
			Epoch:   s.net.CurrentEpoch(),
			Status:  st,
		},
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

func (s *server) makeFailedBalanceResponse(err error, req *protoaccounting.BalanceRequest) (*protoaccounting.BalanceResponse, error) {
	return s.makeBalanceResponse(nil, util.ToStatus(err), req)
}

// Balance gets current balance of the requested user using underlying
// [BalanceContract] and returns result in the response.
func (s *server) Balance(_ context.Context, req *protoaccounting.BalanceRequest) (*protoaccounting.BalanceResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedBalanceResponse(err, nil)
	}

	mUsr := req.GetBody().GetOwnerId()
	if mUsr == nil {
		return s.makeFailedBalanceResponse(errors.New("missing account"), req)
	}
	var id user.ID
	if err := id.FromProtoMessage(mUsr); err != nil {
		return s.makeFailedBalanceResponse(fmt.Errorf("invalid account: %w", err), req)
	}

	bal, err := s.contract.BalanceOf(id)
	if err != nil {
		return s.makeFailedBalanceResponse(err, req)
	}
	ds, err := s.contract.Decimals()
	if err != nil {
		return s.makeFailedBalanceResponse(err, req)
	}

	body := &protoaccounting.BalanceResponse_Body{
		Balance: &protoaccounting.Decimal{Value: bal.Int64(), Precision: ds},
	}
	return s.makeBalanceResponse(body, util.StatusOK, req)
}
