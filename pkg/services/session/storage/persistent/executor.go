package persistent

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.etcd.io/bbolt"
)

// Create inits a new private session token using information
// from corresponding request, saves it to bolt database (and
// encrypts private keys if storage has been configured so).
// Returns response that is filled with just created token's
// ID and public key for it.
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

	value, err := s.packToken(body.GetExpiration(), &sk.PrivateKey)
	if err != nil {
		return nil, err
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)

		ownerBucket, err := rootBucket.CreateBucketIfNotExists(id.WalletBytes())
		if err != nil {
			return fmt.Errorf(
				"could not get/create %s owner bucket: %w", id, err)
		}

		err = ownerBucket.Put(uidBytes, value)
		if err != nil {
			return fmt.Errorf("could not put session token for %s oid: %w", id, err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not save token to persistent storage: %w", err)
	}

	res := new(session.CreateResponseBody)
	res.SetID(uidBytes)
	res.SetSessionKey(sk.PublicKey().Bytes())

	return res, nil
}
