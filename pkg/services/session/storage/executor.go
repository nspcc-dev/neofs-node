package storage

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
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

	sk, err := keys.NewPrivateKey()
	if err != nil {
		return nil, err
	}

	s.mtx.Lock()
	s.tokens[key{
		tokenID: base58.Encode(uidBytes),
		ownerID: base58.Encode(ownerBytes),
	}] = &PrivateToken{
		sessionKey: &sk.PrivateKey,
		exp:        body.GetExpiration(),
	}
	s.mtx.Unlock()

	res := new(session.CreateResponseBody)
	res.SetID(uidBytes)
	res.SetSessionKey(sk.PublicKey().Bytes())

	return res, nil
}
