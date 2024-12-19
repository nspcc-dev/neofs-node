package session

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/google/uuid"
	apirefs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	apisession "github.com/nspcc-dev/neofs-api-go/v2/session"
	protosession "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// KeyStorage represents private keys stored on the local node side.
type KeyStorage interface {
	// Store saves given private key by specified user and locally unique IDs along
	// with its expiration time.
	Store(key *ecdsa.PrivateKey, _ user.ID, id []byte, exp uint64) error
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

func (s *server) makeCreateResponse(body *protosession.CreateResponse_Body, code uint32, msg string) (*protosession.CreateResponse, error) {
	v := version.Current()
	var v2 apirefs.Version
	v.WriteToV2(&v2)
	resp := &protosession.CreateResponse{
		Body: body,
		MetaHeader: &protosession.ResponseMetaHeader{
			Version: v2.ToGRPCMessage().(*refs.Version),
			Epoch:   s.net.CurrentEpoch(),
			Status:  &protostatus.Status{Code: code, Message: msg},
		},
	}
	return util.SignResponse(s.signer, resp, apisession.CreateResponse{}), nil
}

func (s *server) makeFailedCreateResponse(code uint32, err error) (*protosession.CreateResponse, error) {
	return s.makeCreateResponse(nil, code, err.Error())
}

// Create generates new private session key and saves it in the underlying
// [KeyStorage].
func (s *server) Create(_ context.Context, req *protosession.CreateRequest) (*protosession.CreateResponse, error) {
	createReq := new(apisession.CreateRequest)
	if err := createReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	// TODO: use const codes on SDK upgrade
	if err := signature.VerifyServiceMessage(createReq); err != nil {
		return s.makeFailedCreateResponse(1026, err)
	}

	reqBody := req.GetBody()
	mUsr := reqBody.GetOwnerId()
	if mUsr == nil {
		return s.makeFailedCreateResponse(1024, errors.New("missing account"))
	}
	var usr2 apirefs.OwnerID
	if err := usr2.FromGRPCMessage(mUsr); err != nil {
		panic(err)
	}
	var usr user.ID
	if err := usr.ReadFromV2(usr2); err != nil {
		return s.makeFailedCreateResponse(1024, fmt.Errorf("invalid account: %w", err))
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return s.makeFailedCreateResponse(1024, fmt.Errorf("generate private key: %w", err))
	}

	uid := uuid.New()
	if err := s.keys.Store(key, usr, uid[:], reqBody.Expiration); err != nil {
		return s.makeFailedCreateResponse(1024, fmt.Errorf("store private key locally: %w", err))
	}

	body := &protosession.CreateResponse_Body{
		Id:         uid[:],
		SessionKey: neofscrypto.PublicKeyBytes((*neofsecdsa.PublicKey)(&key.PublicKey)),
	}
	return s.makeCreateResponse(body, 0, "")
}
