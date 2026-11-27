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
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	timecore "github.com/nspcc-dev/neofs-node/pkg/core/time"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// KeyStorage represents private keys stored on the local node side.
type KeyStorage interface {
	// Store saves given private key by its account along
	// with its expiration time.
	Store(key ecdsa.PrivateKey, exp uint64) error
}

// maxSessionLifetime bounds how far into the future a new key can be kept in storage.
const maxSessionLifetime = 720

type server struct {
	protosession.UnimplementedSessionServiceServer
	signer    *ecdsa.PrivateKey
	keys      KeyStorage
	chainTime timecore.Provider
	net       netmapcore.State
}

// New provides protosession.SessionServiceServer based on specified [KeyStorage].
//
// All response messages are signed using specified signer and have current
// epoch in the meta header.
func New(s *ecdsa.PrivateKey, ks KeyStorage, chainTime timecore.Provider, net netmapcore.State) protosession.SessionServiceServer {
	return &server{
		signer:    s,
		keys:      ks,
		chainTime: chainTime,
		net:       net,
	}
}

func (s *server) makeCreateResponse(body *protosession.CreateResponse_Body, st *protostatus.Status, req *protosession.CreateRequest) (*protosession.CreateResponse, error) {
	var v *protorefs.Version
	if util.NeedVersionInResponse(req.MetaHeader) {
		v = version.Current().ProtoMessage()
	}

	resp := &protosession.CreateResponse{
		Body: body,
		MetaHeader: &protosession.ResponseMetaHeader{
			Version: v,
			Status:  st,
		},
	}
	return resp, nil
}

func (s *server) makeFailedCreateResponse(err error, req *protosession.CreateRequest) (*protosession.CreateResponse, error) {
	return s.makeCreateResponse(nil, util.ToStatus(err), req)
}

// Create generates new private session key and saves it in the underlying
// [KeyStorage].
func (s *server) Create(_ context.Context, req *protosession.CreateRequest) (*protosession.CreateResponse, error) {
	if err := util.VerifyRequestAPIVersion(req.MetaHeader.GetVersion()); err != nil {
		return s.makeFailedCreateResponse(err, req)
	}
	if err := util.VerifyRequestValidityTime(s.chainTime.Now(), req.MetaHeader); err != nil {
		return s.makeFailedCreateResponse(err, req)
	}

	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedCreateResponse(err, req)
	}

	reqBody := req.GetBody()
	mUsr := reqBody.GetOwnerId()
	if mUsr == nil {
		return s.makeFailedCreateResponse(errors.New("missing account"), req)
	}
	var usr user.ID
	if err := usr.FromProtoMessage(mUsr); err != nil {
		return s.makeFailedCreateResponse(fmt.Errorf("invalid account: %w", err), req)
	}

	cur := s.net.CurrentEpoch()
	if reqBody.Expiration < cur {
		var err apistatus.BadRequest
		err.SetMessage(fmt.Sprintf("requested expiration epoch %d precedes the current epoch %d", reqBody.Expiration, cur))
		return s.makeFailedCreateResponse(err, req)
	}
	if reqBody.Expiration-cur > maxSessionLifetime {
		var err apistatus.BadRequest
		err.SetMessage(fmt.Sprintf("requested expiration epoch %d exceeds the maximum session lifetime of %d epochs from current epoch %d",
			reqBody.Expiration, maxSessionLifetime, cur))
		return s.makeFailedCreateResponse(err, req)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return s.makeFailedCreateResponse(fmt.Errorf("generate private key: %w", err), req)
	}

	if err := s.keys.Store(*key, reqBody.Expiration); err != nil {
		return s.makeFailedCreateResponse(fmt.Errorf("store private key locally: %w", err), req)
	}

	uid := uuid.New()
	body := &protosession.CreateResponse_Body{
		Id:         uid[:],
		SessionKey: neofscrypto.PublicKeyBytes((*neofsecdsa.PublicKey)(&key.PublicKey)),
	}
	return s.makeCreateResponse(body, util.StatusOK, req)
}
