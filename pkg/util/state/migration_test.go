package state

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestMigrateOldTokenStorage(t *testing.T) {
	dir := t.TempDir()
	oldDBPath := filepath.Join(dir, "old-sessions.db")
	newDBPath := filepath.Join(dir, "new-state.db")

	ownerID1 := usertest.ID()
	ownerID2 := usertest.ID()
	exOwnerID := usertest.ID()
	tokenID1 := []byte("token-id-1")
	tokenID2 := []byte("token-id-2")
	tokenID3 := []byte("token-id-3")
	exTokenID := []byte("existing-token")

	key1, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	key2, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	key3, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	exKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	epoch1 := uint64(100)
	epoch2 := uint64(200)
	epoch3 := uint64(300)
	exEpoch := uint64(250)

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
	require.NoError(t, newStorage.Store(*exKey, exOwnerID, exTokenID, exEpoch))

	err = newStorage.MigrateOldTokenStorage(oldDBPath)
	require.NoError(t, err)

	t.Run("verify token 1", func(t *testing.T) {
		token := newStorage.GetToken(ownerID1, tokenID1)
		require.NotNil(t, token)
		require.Equal(t, epoch1, token.ExpiredAt())
		require.Equal(t, key1, token.SessionKey())
	})

	t.Run("verify token 2", func(t *testing.T) {
		token := newStorage.GetToken(ownerID1, tokenID2)
		require.NotNil(t, token)
		require.Equal(t, epoch2, token.ExpiredAt())
		require.Equal(t, key2, token.SessionKey())
	})

	t.Run("verify token 3", func(t *testing.T) {
		token := newStorage.GetToken(ownerID2, tokenID3)
		require.NotNil(t, token)
		require.Equal(t, epoch3, token.ExpiredAt())
		require.Equal(t, key3, token.SessionKey())
	})

	t.Run("existing token preserved", func(t *testing.T) {
		token := newStorage.GetToken(exOwnerID, exTokenID)
		require.NotNil(t, token)
		require.Equal(t, exEpoch, token.ExpiredAt())
		require.Equal(t, exKey, token.SessionKey())
	})

	t.Run("non-existent token", func(t *testing.T) {
		token := newStorage.GetToken(usertest.ID(), []byte("non-existent"))
		require.Nil(t, token)
	})
}

func TestMigrateEmptyOldTokenStorage(t *testing.T) {
	dir := t.TempDir()
	oldDBPath := filepath.Join(dir, "empty-sessions.db")
	newDBPath := filepath.Join(dir, "new-state.db")

	createOldSessionDB(t, oldDBPath, nil)

	newStorage := newStorageWithSession(t, newDBPath)

	err := newStorage.MigrateOldTokenStorage(oldDBPath)
	require.NoError(t, err)

	token := newStorage.GetToken(usertest.ID(), []byte("any-token"))
	require.Nil(t, token)
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

	for ownerID, tokens := range allTokens {
		for tokenIDStr, tokenData := range tokens {
			token := newStorage.GetToken(ownerID, []byte(tokenIDStr))
			require.NotNil(t, token)
			require.Equal(t, tokenData.epoch, token.ExpiredAt())
			require.Equal(t, tokenData.key.D, token.SessionKey().D)
		}
	}
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

	token := newStorage.GetToken(ownerID, tokenID)
	require.NotNil(t, token)
	require.Equal(t, epoch, token.ExpiredAt())
	require.Equal(t, sessionKey.D, token.SessionKey().D)
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
