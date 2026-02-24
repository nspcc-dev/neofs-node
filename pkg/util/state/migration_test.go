package state

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMigrateSessionTokensToAccounts(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sessions.db")

	db, err := bbolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)

	ownerID1 := usertest.ID()
	ownerID2 := usertest.ID()
	ownerID3 := usertest.ID()
	ownerID4 := usertest.ID()

	key1, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	key2, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	key3, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	pubKeyID1 := user.NewFromECDSAPublicKey(key1.PublicKey)
	pubKeyID2 := user.NewFromECDSAPublicKey(key2.PublicKey)
	pubKeyID3 := user.NewFromECDSAPublicKey(key3.PublicKey)

	uuid1 := []byte("0123456789abcdef")
	uuid2 := []byte("fedcba9876543210")

	epoch1 := uint64(100)
	epoch2 := uint64(200)
	epoch3 := uint64(300)

	err = db.Update(func(tx *bbolt.Tx) error {
		rootBucket, err := tx.CreateBucketIfNotExists(sessionsBucket)
		require.NoError(t, err)

		tempStorage := &PersistentStorage{db: db}

		// Owner 1 with one UUID token
		ownerBucket1, err := rootBucket.CreateBucket(ownerID1[:])
		require.NoError(t, err)

		packedToken1, err := tempStorage.packToken(epoch1, key1)
		require.NoError(t, err)

		err = ownerBucket1.Put(uuid1, packedToken1)
		require.NoError(t, err)

		// Owner 2 with one token that has both UUID and pubkey ID entries
		ownerBucket2, err := rootBucket.CreateBucket(ownerID2[:])
		require.NoError(t, err)

		packedToken2, err := tempStorage.packToken(epoch2, key2)
		require.NoError(t, err)
		err = ownerBucket2.Put(uuid2, packedToken2)
		require.NoError(t, err)
		err = ownerBucket2.Put(pubKeyID2[:], packedToken2)
		require.NoError(t, err)

		// Owner 3 with one token with pubkey ID
		ownerBucket3, err := rootBucket.CreateBucket(ownerID3[:])
		require.NoError(t, err)

		packedToken3, err := tempStorage.packToken(epoch3, key3)
		require.NoError(t, err)
		err = ownerBucket3.Put(pubKeyID3[:], packedToken3)
		require.NoError(t, err)

		// Owner 4 with no tokens
		_, err = rootBucket.CreateBucket(ownerID4[:])
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)
	_ = db.Close()

	logger, logBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)
	storage, err := NewPersistentStorage(dbPath, true, WithLogger(logger))
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	require.NoError(t, storage.MigrateSessionTokensToAccounts())

	logBuf.AssertContains(testutil.LogEntry{
		Level:   zap.InfoLevel,
		Message: "session token storage migration completed",
		Fields: map[string]any{
			"migrated": json.Number("3"),
			"deleted":  json.Number("4"),
		},
	})

	token1 := storage.GetToken(pubKeyID1)
	require.NotNil(t, token1)
	require.Equal(t, epoch1, token1.ExpiredAt())
	require.Equal(t, key1, token1.SessionKey())

	token2 := storage.GetToken(pubKeyID2)
	require.NotNil(t, token2)
	require.Equal(t, epoch2, token2.ExpiredAt())
	require.Equal(t, key2, token2.SessionKey())

	token3 := storage.GetToken(pubKeyID3)
	require.NotNil(t, token3)
	require.Equal(t, epoch3, token3.ExpiredAt())
	require.Equal(t, key3, token3.SessionKey())

	require.NoError(t, storage.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}

		require.Nil(t, rootBucket.Bucket(ownerID1[:]))
		require.Nil(t, rootBucket.Bucket(ownerID2[:]))
		require.Nil(t, rootBucket.Bucket(ownerID3[:]))
		require.Nil(t, rootBucket.Bucket(ownerID4[:]))

		return nil
	}))

	require.NoError(t, storage.MigrateSessionTokensToAccounts())
	logBuf.AssertContainsMsg(zap.DebugLevel, "session token storage migration is not needed, already migrated")
}
