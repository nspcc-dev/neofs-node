package temporary

import (
	"context"
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

func (s *TokenStore) Create(_ context.Context, body *session.CreateRequestBody) (*session.CreateResponseBody, error) {
	idV2 := body.GetOwnerID()
	if idV2 == nil {
		return nil, errors.New("missing owner")
	}

	var id user.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, fmt.Errorf("invalid owner: %w", err)
	}

	uidBytes, err := storage.NewTokenID()
	if err != nil {
		return nil, fmt.Errorf("could not generate token ID: %w", err)
	}

	sk, err := keys.NewPrivateKey()
	if err != nil {
		return nil, err
	}

	s.mtx.Lock()
	s.tokens[key{
		tokenID: base58.Encode(uidBytes),
		ownerID: base58.Encode(id.WalletBytes()),
	}] = storage.NewPrivateToken(&sk.PrivateKey, body.GetExpiration())
	s.mtx.Unlock()

	res := new(session.CreateResponseBody)
	res.SetID(uidBytes)
	res.SetSessionKey(sk.PublicKey().Bytes())

	return res, nil
}

func (s *TokenStore) Close() error {
	return nil
}
