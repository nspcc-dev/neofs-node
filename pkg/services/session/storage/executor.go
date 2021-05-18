package storage

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	crypto "github.com/nspcc-dev/neofs-crypto"
)

func (s *TokenStore) Create(ctx context.Context, body *session.CreateRequestBody) (*session.CreateResponseBody, error) {
	ownerBytes, err := owner.NewIDFromV2(body.GetOwnerID()).Marshal()
	if err != nil {
		panic(err)
	}

	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("could not generate token ID: %w", err)
	}

	uidBytes, err := uid.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("could not marshal token ID: %w", err)
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
