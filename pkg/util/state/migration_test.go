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

func TestMigrateOldTokenStorage(t *testing.T) {
	dir := t.TempDir()
	oldDBPath := filepath.Join(dir, "old-sessions.db")
	newDBPath := filepath.Join(dir, "new-state.db")

	ownerID1 := usertest.ID()
	ownerID2 := usertest.ID()
	tokenID1 := []byte("token-id-1")
	tokenID2 := []byte("token-id-2")
	tokenID3 := []byte("token-id-3")

	key1, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	key2, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	key3, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	epoch1 := uint64(100)
	epoch2 := uint64(200)
	epoch3 := uint64(300)

	createOldSessionDB(t, oldDBPath, map[user.ID]map[string]tokenData{
		ownerID1: {
			string(tokenID1): {key: key1, epoch: epoch1},
			string(tokenID2): {key: key2, epoch: epoch2},
		},
		ownerID2: {
			string(tokenID3): {key: key3, epoch: epoch3},
		},
	})

	newStorage := newStorageWithSession(t, newDBPath)

	err = newStorage.MigrateOldTokenStorage(oldDBPath)
	require.NoError(t, err)

	require.NoError(t, newStorage.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		require.NotNil(t, rootBucket)

		owner1Bucket := rootBucket.Bucket(ownerID1[:])
		require.NotNil(t, owner1Bucket)
		require.NotNil(t, owner1Bucket.Get(tokenID1))
		require.NotNil(t, owner1Bucket.Get(tokenID2))

		owner2Bucket := rootBucket.Bucket(ownerID2[:])
		require.NotNil(t, owner2Bucket)
		require.NotNil(t, owner2Bucket.Get(tokenID3))

		require.Nil(t, owner1Bucket.Get([]byte("non-existent")))

		return nil
	}))
}

func TestMigrateEmptyOldTokenStorage(t *testing.T) {
	dir := t.TempDir()
	oldDBPath := filepath.Join(dir, "empty-sessions.db")
	newDBPath := filepath.Join(dir, "new-state.db")

	createOldSessionDB(t, oldDBPath, nil)

	newStorage := newStorageWithSession(t, newDBPath)

	err := newStorage.MigrateOldTokenStorage(oldDBPath)
	require.NoError(t, err)

	require.NoError(t, newStorage.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}
		c := rootBucket.Cursor()
		k, v := c.First()
		require.Nil(t, k)
		require.Nil(t, v)
		return nil
	}))
}

func TestMigrateOldTokenStorageMultipleOwners(t *testing.T) {
	dir := t.TempDir()
	oldDBPath := filepath.Join(dir, "old-sessions.db")
	newDBPath := filepath.Join(dir, "new-state.db")

	owners := make([]user.ID, 5)
	tokensPerOwner := 3
	allTokens := make(map[user.ID]map[string]tokenData)

	for i := range owners {
		owners[i] = usertest.ID()
		allTokens[owners[i]] = make(map[string]tokenData)

		for j := range tokensPerOwner {
			tokenID := []byte{byte('t'), byte(i), byte(j)}
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			require.NoError(t, err)
			epoch := uint64((i+1)*100 + j)

			allTokens[owners[i]][string(tokenID)] = tokenData{key: key, epoch: epoch}
		}
	}

	createOldSessionDB(t, oldDBPath, allTokens)

	newStorage := newStorageWithSession(t, newDBPath)
	err := newStorage.MigrateOldTokenStorage(oldDBPath)
	require.NoError(t, err)

	require.NoError(t, newStorage.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		require.NotNil(t, rootBucket)

		for _, ownerID := range owners {
			ownerBucket := rootBucket.Bucket(ownerID[:])
			require.NotNil(t, ownerBucket)

			for tokenIDStr := range allTokens[ownerID] {
				tokenID := []byte(tokenIDStr)
				require.NotNil(t, ownerBucket.Get(tokenID))
			}
		}
		return nil
	}))
}

func TestMigrateOldTokenStorageNonExistentFile(t *testing.T) {
	dir := t.TempDir()
	oldDBPath := filepath.Join(dir, "non-existent.db")
	newDBPath := filepath.Join(dir, "new-state.db")

	newStorage := newStorageWithSession(t, newDBPath)
	err := newStorage.MigrateOldTokenStorage(oldDBPath)
	require.Error(t, err)
}

func TestMigrateOldTokenStorageWithEncryption(t *testing.T) {
	dir := t.TempDir()
	oldDBPath := filepath.Join(dir, "old-sessions.db")
	newDBPath := filepath.Join(dir, "new-state.db")

	encKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	ownerID := usertest.ID()
	tokenID := []byte("encrypted-token")
	sessionKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	epoch := uint64(150)

	createOldSessionDBWithEncryption(t, oldDBPath, encKey, map[user.ID]map[string]tokenData{
		ownerID: {
			string(tokenID): {key: sessionKey, epoch: epoch},
		},
	})

	newStorage := newStorageWithSession(t, newDBPath, WithEncryptionKey(encKey))

	err = newStorage.MigrateOldTokenStorage(oldDBPath)
	require.NoError(t, err)

	require.NoError(t, newStorage.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		require.NotNil(t, rootBucket)

		ownerBucket := rootBucket.Bucket(ownerID[:])
		require.NotNil(t, ownerBucket)
		require.NotNil(t, ownerBucket.Get(tokenID))

		return nil
	}))
}

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

type tokenData struct {
	key   *ecdsa.PrivateKey
	epoch uint64
}

func createOldSessionDB(t *testing.T, path string, tokens map[user.ID]map[string]tokenData) {
	createOldSessionDBWithEncryption(t, path, nil, tokens)
}

func createOldSessionDBWithEncryption(t *testing.T, path string, encKey *ecdsa.PrivateKey, tokens map[user.ID]map[string]tokenData) {
	db, err := bbolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tempStorage := &PersistentStorage{db: db}
	if encKey != nil {
		tempCfg := defaultCfg()
		tempCfg.privateKey = encKey
		err = tempStorage.initTokenStore(*tempCfg)
		require.NoError(t, err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		rootBucket, err := tx.CreateBucketIfNotExists(sessionsBucket)
		require.NoError(t, err)

		for ownerID, ownerTokens := range tokens {
			ownerBucket, err := rootBucket.CreateBucket(ownerID[:])
			require.NoError(t, err)

			for tokenIDStr, tokenData := range ownerTokens {
				packedToken, err := tempStorage.packToken(tokenData.epoch, tokenData.key)
				require.NoError(t, err)

				err = ownerBucket.Put([]byte(tokenIDStr), packedToken)
				require.NoError(t, err)
			}
		}

		return nil
	})
	require.NoError(t, err)
}
