package persistent

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"go.etcd.io/bbolt"
)

// Create inits a new private session token using information
// from corresponding request, saves it to bolt database (and
// encrypts private keys if storage has been configured so).
// Returns response that is filled with just created token's
// ID and public key for it.
func (s *TokenStore) Create(ctx context.Context, body *session.CreateRequestBody) (*session.CreateResponseBody, error) {
	ownerBytes, err := owner.NewIDFromV2(body.GetOwnerID()).Marshal()
	if err != nil {
		panic(err)
	}

	uidBytes, err := storage.NewTokenID()
	if err != nil {
		return nil, fmt.Errorf("could not generate token ID: %w", err)
	}

	sk, err := keys.NewPrivateKey()
	if err != nil {
		return nil, err
	}

	value, err := packToken(body.GetExpiration(), &sk.PrivateKey)
	if err != nil {
		return nil, err
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)

		ownerBucket, err := rootBucket.CreateBucketIfNotExists(ownerBytes)
		if err != nil {
			return fmt.Errorf(
				"could not get/create %s owner bucket: %w",
				hex.EncodeToString(ownerBytes),
				err,
			)
		}

		err = ownerBucket.Put(uidBytes, value)
		if err != nil {
			return fmt.Errorf("could not put session token for %s oid: %w",
				hex.EncodeToString(ownerBytes),
				err,
			)
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
