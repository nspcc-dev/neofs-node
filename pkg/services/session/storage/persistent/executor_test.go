package persistent

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestTokenStore(t *testing.T) {
	ts, err := NewTokenStore(filepath.Join(t.TempDir(), ".storage"))
	require.NoError(t, err)

	defer ts.Close()

	const tokenNumber = 5

	type tok struct {
		owner user.ID
		id    []byte
		key   ecdsa.PrivateKey
	}

	tokens := make([]tok, 0, tokenNumber)

	for i := range tokenNumber {
		usr := usertest.User()
		sessionID := make([]byte, 32) // any len
		_, _ = rand.Read(sessionID)

		err := ts.Store(usr.ECDSAPrivateKey, usr.ID, sessionID, uint64(i))
		require.NoError(t, err)

		tokens = append(tokens, tok{
			owner: usr.ID,
			id:    sessionID,
			key:   usr.ECDSAPrivateKey,
		})
	}

	for i, token := range tokens {
		savedToken := ts.Get(token.owner, token.id)

		require.Equal(t, uint64(i), savedToken.ExpiredAt())
		require.NotNil(t, savedToken.SessionKey())
		require.Equal(t, token.key, *savedToken.SessionKey())
	}
}

func TestTokenStore_Persistent(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".storage")

	ts, err := NewTokenStore(path)
	require.NoError(t, err)

	sessionID := make([]byte, 64) // any len
	owner := usertest.User()
	const exp = 12345

	err = ts.Store(owner.ECDSAPrivateKey, owner.ID, sessionID, exp)
	require.NoError(t, err)

	// close db (stop the node)
	require.NoError(t, ts.Close())

	// open persistent storage again
	ts, err = NewTokenStore(path)
	require.NoError(t, err)

	defer ts.Close()

	savedToken := ts.Get(owner.ID, sessionID)

	require.EqualValues(t, exp, savedToken.ExpiredAt())
	require.NotNil(t, savedToken.SessionKey())
	require.Equal(t, owner.ECDSAPrivateKey, *savedToken.SessionKey())
}

func TestTokenStore_RemoveOld(t *testing.T) {
	tests := []*struct {
		epoch uint64
		owner user.ID
		id    []byte
		key   ecdsa.PrivateKey
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

	for _, test := range tests {
		test.id = make([]byte, 32) // any len
		_, _ = rand.Read(test.id)
		owner := usertest.User()

		err := ts.Store(owner.ECDSAPrivateKey, owner.ID, test.id, test.epoch)
		require.NoError(t, err)

		test.owner = owner.ID
		test.key = owner.ECDSAPrivateKey
	}

	const currEpoch = 3

	ts.RemoveOld(currEpoch)

	for _, test := range tests {
		token := ts.Get(test.owner, test.id)

		if test.epoch <= currEpoch {
			require.Nil(t, token)
		} else {
			require.EqualValues(t, test.epoch, token.ExpiredAt())
			require.NotNil(t, token.SessionKey())
			require.Equal(t, test.key, *token.SessionKey())
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
