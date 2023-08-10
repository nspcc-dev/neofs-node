package persistent

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestTokenStore(t *testing.T) {
	ts, err := NewTokenStore(filepath.Join(t.TempDir(), ".storage"))
	require.NoError(t, err)

	defer ts.Close()

	owner := usertest.ID(t)

	var ownerV2 refs.OwnerID
	owner.WriteToV2(&ownerV2)

	req := new(session.CreateRequestBody)
	req.SetOwnerID(&ownerV2)

	const tokenNumber = 5

	type tok struct {
		id  []byte
		key []byte
	}

	tokens := make([]tok, 0, tokenNumber)

	for i := 0; i < tokenNumber; i++ {
		req.SetExpiration(uint64(i))

		res, err := ts.Create(context.Background(), req)
		require.NoError(t, err)

		tokens = append(tokens, tok{
			id:  res.GetID(),
			key: res.GetSessionKey(),
		})
	}

	for i, token := range tokens {
		savedToken := ts.Get(owner, token.id)

		require.Equal(t, uint64(i), savedToken.ExpiredAt())

		equalKeys(t, token.key, savedToken.SessionKey())
	}
}

func TestTokenStore_Persistent(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".storage")

	ts, err := NewTokenStore(path)
	require.NoError(t, err)

	idOwner := usertest.ID(t)

	var idOwnerV2 refs.OwnerID
	idOwner.WriteToV2(&idOwnerV2)

	const exp = 12345

	req := new(session.CreateRequestBody)
	req.SetOwnerID(&idOwnerV2)
	req.SetExpiration(exp)

	res, err := ts.Create(context.Background(), req)
	require.NoError(t, err)

	id := res.GetID()
	pubKey := res.GetSessionKey()

	// close db (stop the node)
	require.NoError(t, ts.Close())

	// open persistent storage again
	ts, err = NewTokenStore(path)
	require.NoError(t, err)

	defer ts.Close()

	savedToken := ts.Get(idOwner, id)

	equalKeys(t, pubKey, savedToken.SessionKey())
}

func TestTokenStore_RemoveOld(t *testing.T) {
	tests := []*struct {
		epoch   uint64
		id, key []byte
	}{
		{
			epoch: 1,
		},
		{
			epoch: 2,
		},
		{
			epoch: 3,
		},
		{
			epoch: 4,
		},
		{
			epoch: 5,
		},
		{
			epoch: 6,
		},
	}

	ts, err := NewTokenStore(filepath.Join(t.TempDir(), ".storage"))
	require.NoError(t, err)

	defer ts.Close()

	owner := usertest.ID(t)

	var ownerV2 refs.OwnerID
	owner.WriteToV2(&ownerV2)

	req := new(session.CreateRequestBody)
	req.SetOwnerID(&ownerV2)

	for _, test := range tests {
		req.SetExpiration(test.epoch)

		res, err := ts.Create(context.Background(), req)
		require.NoError(t, err)

		test.id = res.GetID()
		test.key = res.GetSessionKey()
	}

	const currEpoch = 3

	ts.RemoveOld(currEpoch)

	for _, test := range tests {
		token := ts.Get(owner, test.id)

		if test.epoch <= currEpoch {
			require.Nil(t, token)
		} else {
			equalKeys(t, test.key, token.SessionKey())
		}
	}
}

// This test was added to fix bolt's behaviour since the persistent
// storage uses cursor and there is an issue about `cursor.Delete`
// method: https://github.com/etcd-io/bbolt/issues/146.
//
// If this test is passing, TokenStore works correctly.
func TestBolt_Cursor(t *testing.T) {
	db, err := bbolt.Open(filepath.Join(t.TempDir(), ".storage"), 0666, nil)
	require.NoError(t, err)

	defer db.Close()

	cursorKeys := make(map[string]struct{})

	var bucketName = []byte("bucket")

	err = db.Update(func(tx *bbolt.Tx) (err error) {
		b, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}

		put := func(s []byte) {
			if err == nil {
				err = b.Put(s, s)
			}
		}

		put([]byte("1"))
		put([]byte("2"))
		put([]byte("3"))
		put([]byte("4"))

		return
	})

	err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			// fill key that was viewed
			cursorKeys[string(k)] = struct{}{}

			if bytes.Equal(k, []byte("1")) {
				// delete the first one
				err = c.Delete()
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
	require.NoError(t, err)

	_, ok := cursorKeys["2"]
	if !ok {
		t.Fatal("unexpectedly skipped '2' value")
	}
}

func equalKeys(t *testing.T, sessionKey []byte, savedPrivateKey *ecdsa.PrivateKey) {
	returnedPubKey, err := keys.NewPublicKeyFromBytes(sessionKey, elliptic.P256())
	require.NoError(t, err)

	savedPubKey := (keys.PublicKey)(savedPrivateKey.PublicKey)

	require.Equal(t, true, returnedPubKey.Equal(&savedPubKey))
}
