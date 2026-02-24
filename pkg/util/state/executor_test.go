package state

import (
	"bytes"
	"crypto/ecdsa"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/bbolt"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestTokenStore(t *testing.T) {
	ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"))

	const tokenNumber = 5

	type tok struct {
		user usertest.UserSigner
	}

	tokens := make([]tok, 0, tokenNumber)

	for i := range tokenNumber {
		usr := usertest.User()

		err := ts.Store(usr.ECDSAPrivateKey, uint64(i))
		require.NoError(t, err)

		tokens = append(tokens, tok{
			user: usr,
		})
	}

	for i, token := range tokens {
		savedToken := ts.GetToken(token.user.UserID())

		require.Equal(t, uint64(i), savedToken.ExpiredAt())
		require.NotNil(t, savedToken.SessionKey())
		require.Equal(t, token.user.ECDSAPrivateKey, *savedToken.SessionKey())
	}
}

func TestTokenStore_Persistent(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".storage")
	ts := newStorageWithSession(t, path)

	account := usertest.User()
	const exp = 12345

	err := ts.Store(account.ECDSAPrivateKey, exp)
	require.NoError(t, err)

	// close db (stop the node)
	require.NoError(t, ts.Close())

	// open persistent storage again
	ts = newStorageWithSession(t, path)

	savedToken := ts.GetToken(account.ID)

	require.EqualValues(t, exp, savedToken.ExpiredAt())
	require.NotNil(t, savedToken.SessionKey())
	require.Equal(t, account.ECDSAPrivateKey, *savedToken.SessionKey())
}

func TestTokenStore_RemoveOld(t *testing.T) {
	tests := []*struct {
		epoch   uint64
		account user.ID
		key     ecdsa.PrivateKey
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

	ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"))

	for _, test := range tests {
		acc := usertest.User()

		err := ts.Store(acc.ECDSAPrivateKey, test.epoch)
		require.NoError(t, err)

		test.account = acc.ID
		test.key = acc.ECDSAPrivateKey
	}

	const currEpoch = 3

	ts.RemoveOldTokens(currEpoch)

	for _, test := range tests {
		token := ts.GetToken(test.account)

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

func TestTokenStore_FindTokenBySubjects(t *testing.T) {
	ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"))

	const tokenNumber = 3
	tokens := make([]struct {
		account user.ID
		key     ecdsa.PrivateKey
	}, tokenNumber)

	for i := range tokenNumber {
		subject := usertest.User()

		err := ts.Store(subject.ECDSAPrivateKey, uint64(100+i))
		require.NoError(t, err)

		tokens[i].account = subject.ID
		tokens[i].key = subject.ECDSAPrivateKey
	}

	subjects := make([]sessionv2.Target, 0, tokenNumber)
	for _, tok := range tokens {
		userID := user.NewFromECDSAPublicKey(tok.key.PublicKey)
		subjects = append(subjects, sessionv2.NewTargetUser(userID))
	}

	for i, tok := range tokens {
		foundToken := ts.FindTokenBySubjects([]sessionv2.Target{subjects[i]})
		require.NotNil(t, foundToken)
		require.EqualValues(t, 100+i, foundToken.ExpiredAt())
		require.Equal(t, tok.key, *foundToken.SessionKey())
	}

	// first matching subject in db
	foundToken := ts.FindTokenBySubjects(subjects)
	require.NotNil(t, foundToken)
	require.EqualValues(t, 100, foundToken.ExpiredAt())

	nonExistentSubject := sessionv2.NewTargetUser(usertest.ID())
	foundToken = ts.FindTokenBySubjects([]sessionv2.Target{nonExistentSubject})
	require.Nil(t, foundToken)

	foundToken = ts.FindTokenBySubjects([]sessionv2.Target{})
	require.Nil(t, foundToken)
}
