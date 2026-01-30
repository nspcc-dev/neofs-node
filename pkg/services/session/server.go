package session

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/google/uuid"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// KeyStorage represents private keys stored on the local node side.
type KeyStorage interface {
	// Store saves given private key by specified user and locally unique IDs along
	// with its expiration time.
	Store(key ecdsa.PrivateKey, _ user.ID, id []byte, exp uint64) error
}

type server struct {
	protosession.UnimplementedSessionServiceServer
	signer *ecdsa.PrivateKey
	net    netmap.State
	keys   KeyStorage
}

// New provides protosession.SessionServiceServer based on specified [KeyStorage].
//
// All response messages are signed using specified signer and have current
// epoch in the meta header.
func New(s *ecdsa.PrivateKey, net netmap.State, ks KeyStorage) protosession.SessionServiceServer {
	return &server{
		signer: s,
		net:    net,
		keys:   ks,
	}
}

func (s *server) makeCreateResponse(body *protosession.CreateResponse_Body, st *protostatus.Status) (*protosession.CreateResponse, error) {
	resp := &protosession.CreateResponse{
		Body: body,
		MetaHeader: &protosession.ResponseMetaHeader{
			Version: version.Current().ProtoMessage(),
			Epoch:   s.net.CurrentEpoch(),
			Status:  st,
		},
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *server) makeFailedCreateResponse(err error) (*protosession.CreateResponse, error) {
	return s.makeCreateResponse(nil, util.ToStatus(err))
}

// Create generates new private session key and saves it in the underlying
// [KeyStorage].
func (s *server) Create(_ context.Context, req *protosession.CreateRequest) (*protosession.CreateResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedCreateResponse(err)
	}

	reqBody := req.GetBody()
	mUsr := reqBody.GetOwnerId()
	if mUsr == nil {
		return s.makeFailedCreateResponse(errors.New("missing account"))
	}
	var usr user.ID
	if err := usr.FromProtoMessage(mUsr); err != nil {
		return s.makeFailedCreateResponse(fmt.Errorf("invalid account: %w", err))
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return s.makeFailedCreateResponse(fmt.Errorf("generate private key: %w", err))
	}

	uid := uuid.New()
	if err := s.keys.Store(*key, usr, uid[:], reqBody.Expiration); err != nil {
		return s.makeFailedCreateResponse(fmt.Errorf("store private key locally: %w", err))
	}

	// also store the key using account as key ID
	keyUser := user.NewFromECDSAPublicKey(key.PublicKey)
	if err := s.keys.Store(*key, usr, keyUser[:], reqBody.Expiration); err != nil {
		return s.makeFailedCreateResponse(fmt.Errorf("store private key with public key locally: %w", err))
	}

	body := &protosession.CreateResponse_Body{
		Id:         uid[:],
		SessionKey: neofscrypto.PublicKeyBytes((*neofsecdsa.PublicKey)(&key.PublicKey)),
	}
	return s.makeCreateResponse(body, util.StatusOK)
}
