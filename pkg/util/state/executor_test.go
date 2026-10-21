package state

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/bbolt"
	bolterrors "github.com/nspcc-dev/bbolt/errors"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestTokenStore(t *testing.T) {
	t.Run("store and retrieve", func(t *testing.T) {
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
	})

	t.Run("replace index", func(t *testing.T) {
		ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"))
		account := usertest.User()

		for _, epoch := range []uint64{256, 255, 300, 300} {
			require.NoError(t, ts.Store(account.ECDSAPrivateKey, epoch))
			require.NoError(t, ts.db.View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(sessionsBucket)
				require.NotNil(t, b.Get(tokenKey(account.ID)))
				require.NotNil(t, b.Get(tokenEpochKey(epoch, account.ID)))
				c := b.Cursor()
				count := 0
				for k, _ := c.Seek([]byte{sessionEpochPrefix}); len(k) > 0 && k[0] == sessionEpochPrefix; k, _ = c.Next() {
					count++
				}
				require.Equal(t, 1, count)
				return nil
			}))
		}
		ts.RemoveOldTokens(256)
		require.NotNil(t, ts.GetToken(account.ID))
		require.EqualValues(t, 300, ts.GetToken(account.ID).ExpiredAt())
		ts.RemoveOldTokens(300)
		require.Nil(t, ts.GetToken(account.ID))
		require.NoError(t, ts.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(sessionsBucket)
			require.Nil(t, b.Get(tokenKey(account.ID)))
			require.Nil(t, b.Get(tokenEpochKey(300, account.ID)))
			return nil
		}))
	})

	t.Run("store rollback", func(t *testing.T) {
		ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"))
		account := usertest.User()
		require.NoError(t, ts.Store(account.ECDSAPrivateKey, 1))
		// Make the new index unwritable after Store has removed the old index.
		require.NoError(t, ts.db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.Bucket(sessionsBucket).CreateBucket(tokenEpochKey(2, account.ID))
			return err
		}))
		require.ErrorIs(t, ts.Store(account.ECDSAPrivateKey, 2), bolterrors.ErrIncompatibleValue)
		require.EqualValues(t, 1, ts.GetToken(account.ID).ExpiredAt())
		require.NoError(t, ts.db.View(func(tx *bbolt.Tx) error {
			require.NotNil(t, tx.Bucket(sessionsBucket).Get(tokenEpochKey(1, account.ID)))
			return nil
		}))
	})
}

func TestTokenStore_Migration(t *testing.T) {
	prepareLegacy := func(t *testing.T, ts *PersistentStorage, populate func(*bbolt.Bucket) error) {
		t.Helper()
		require.NoError(t, ts.db.Update(func(tx *bbolt.Tx) error {
			return populate(tx.Bucket(sessionsBucket))
		}))
		require.NoError(t, ts.Close())
	}

	t.Run("empty", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), ".storage")
		ts := newStorageWithSession(t, path)
		require.NoError(t, ts.Close())

		ts = newStorageWithSession(t, path)
		require.NoError(t, ts.db.View(func(tx *bbolt.Tx) error {
			require.Zero(t, tx.Bucket(sessionsBucket).Stats().KeyN)
			return nil
		}))
		account := usertest.User()
		require.NoError(t, ts.Store(account.ECDSAPrivateKey, 1))
		ts.RemoveOldTokens(1)
		require.NoError(t, ts.Close())

		ts = newStorageWithSession(t, path)
		require.NoError(t, ts.db.View(func(tx *bbolt.Tx) error {
			require.Zero(t, tx.Bucket(sessionsBucket).Stats().KeyN)
			return nil
		}))
	})

	t.Run("valid", func(t *testing.T) {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), ".storage")
				var opts []Option
				if encrypted {
					key := usertest.User()
					opts = append(opts, WithEncryptionKey(&key.ECDSAPrivateKey))
				}
				ts := newStorageWithSession(t, path, opts...)
				epochs := []uint64{0, 255, 256, 256, math.MaxUint64}
				accounts := make([]usertest.UserSigner, len(epochs))
				values := make([][]byte, len(epochs))
				for i, epoch := range epochs {
					accounts[i] = usertest.User()
					var err error
					values[i], err = ts.packToken(epoch, &accounts[i].ECDSAPrivateKey)
					require.NoError(t, err)
				}
				require.NoError(t, ts.SetBytes([]byte("unrelated"), []byte("state")))
				// Populate the exact pre-migration layout, including encrypted values.
				prepareLegacy(t, ts, func(b *bbolt.Bucket) error {
					for i, account := range accounts {
						if err := b.Put(account.ID[:], values[i]); err != nil {
							return err
						}
					}
					return nil
				})

				// A second reopen must neither repeat the migration nor change values.
				for range 2 {
					ts = newStorageWithSession(t, path, opts...)
					require.NoError(t, ts.db.View(func(tx *bbolt.Tx) error {
						b := tx.Bucket(sessionsBucket)
						require.Equal(t, 2*len(accounts), b.Stats().KeyN)
						for i, account := range accounts {
							require.Nil(t, b.Get(account.ID[:]))
							require.Equal(t, values[i], b.Get(tokenKey(account.ID)))
							require.NotNil(t, b.Get(tokenEpochKey(epochs[i], account.ID)))
						}
						return nil
					}))
					for i, account := range accounts {
						token := ts.GetToken(account.ID)
						require.NotNil(t, token)
						require.Equal(t, epochs[i], token.ExpiredAt())
						require.Equal(t, account.ECDSAPrivateKey, *token.SessionKey())
						target := sessionv2.NewTargetUser(account.ID)
						require.Equal(t, token, ts.FindTokenBySubjects([]sessionv2.Target{target}))
					}
					value, err := ts.Bytes([]byte("unrelated"))
					require.NoError(t, err)
					require.Equal(t, []byte("state"), value)
					require.NoError(t, ts.Close())
				}
				ts = newStorageWithSession(t, path, opts...)
				ts.RemoveOldTokens(256)
				for i, account := range accounts {
					if epochs[i] <= 256 {
						require.Nil(t, ts.GetToken(account.ID))
					} else {
						require.NotNil(t, ts.GetToken(account.ID))
					}
				}
			})
		}
	})

	t.Run("invalid", func(t *testing.T) {
		for _, invalidKey := range []bool{false, true} {
			t.Run(fmt.Sprintf("invalid key=%t", invalidKey), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), ".storage")
				ts := newStorageWithSession(t, path)
				account := usertest.User()
				raw, err := ts.packToken(256, &account.ECDSAPrivateKey)
				require.NoError(t, err)
				badAccount := usertest.ID()
				badKey, badValue := badAccount[:], []byte{1}
				if invalidKey {
					badKey, badValue = []byte("invalid"), raw
				}
				prepareLegacy(t, ts, func(b *bbolt.Bucket) error {
					if err := b.Put(account.ID[:], raw); err != nil {
						return err
					}
					return b.Put(badKey, badValue)
				})
				_, err = NewPersistentStorage(path, true)
				require.ErrorContains(t, err, "invalid legacy session")
				db, err := bbolt.Open(path, 0o600, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				require.NoError(t, db.View(func(tx *bbolt.Tx) error {
					b := tx.Bucket(sessionsBucket)
					require.Equal(t, 2, b.Stats().KeyN)
					require.Nil(t, b.Get(tokenKey(account.ID)))
					require.Equal(t, raw, b.Get(account.ID[:]))
					require.Equal(t, badValue, b.Get(badKey))
					return nil
				}))
			})
		}
	})
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
	t.Run("expired tokens", func(t *testing.T) {
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
	})

	t.Run("epoch boundaries", func(t *testing.T) {
		ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"))
		ts.RemoveOldTokens(0)

		epochs := []uint64{0, 1, 255, 256, 256, 257, math.MaxUint64}
		accounts := make([]usertest.UserSigner, len(epochs))
		for i, epoch := range epochs {
			accounts[i] = usertest.User()
			require.NoError(t, ts.Store(accounts[i].ECDSAPrivateKey, epoch))
		}
		for _, current := range []uint64{0, 255, 256, 257, math.MaxUint64} {
			ts.RemoveOldTokens(current)
			for i, account := range accounts {
				token := ts.GetToken(account.ID)
				if epochs[i] <= current {
					require.Nil(t, token)
				} else {
					require.NotNil(t, token)
					require.Equal(t, epochs[i], token.ExpiredAt())
				}
			}
		}
		ts.RemoveOldTokens(math.MaxUint64)
		require.NoError(t, ts.db.View(func(tx *bbolt.Tx) error {
			c := tx.Bucket(sessionsBucket).Cursor()
			k, _ := c.Seek([]byte{sessionTokenPrefix})
			require.True(t, len(k) == 0 || k[0] > sessionEpochPrefix)
			return nil
		}))
	})

	t.Run("rollback", func(t *testing.T) {
		ts := newStorageWithSession(t, filepath.Join(t.TempDir(), ".storage"))
		account := usertest.User()
		require.NoError(t, ts.Store(account.ECDSAPrivateKey, 1))
		// The second entry fails after the first token/index pair has been removed.
		require.NoError(t, ts.db.Update(func(tx *bbolt.Tx) error {
			key := tokenEpochKey(2, account.ID)
			return tx.Bucket(sessionsBucket).Put(key[:sessionEpochAccountOffset], nil)
		}))
		ts.RemoveOldTokens(2)
		require.NotNil(t, ts.GetToken(account.ID))
		require.NoError(t, ts.db.View(func(tx *bbolt.Tx) error {
			require.NotNil(t, tx.Bucket(sessionsBucket).Get(tokenEpochKey(1, account.ID)))
			return nil
		}))
	})
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
