package storage

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/pkg/errors"
)

func (s *TokenStore) Create(ctx context.Context, body *session.CreateRequestBody) (*session.CreateResponseBody, error) {
	ownerBytes, err := body.GetOwnerID().StableMarshal(nil)
	if err != nil {
		panic(err)
	}

	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, errors.Wrap(err, "could not generate token ID")
	}

	uidBytes, err := uid.MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal token ID")
	}

	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	s.mtx.Lock()
	s.tokens[key{
		tokenID: base58.Encode(uidBytes),
		ownerID: base58.Encode(ownerBytes),
	}] = &PrivateToken{
		sessionKey: sk,
		exp:        body.GetExpiration(),
	}
	s.mtx.Unlock()

	res := new(session.CreateResponseBody)
	res.SetID(uidBytes)
	res.SetSessionKey(
		crypto.MarshalPublicKey(&sk.PublicKey),
	)

	return res, nil
}
