package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

type epochStateImpl struct{}

func (s epochStateImpl) CurrentEpoch() uint64 {
	return 0
}

func TestVersion(t *testing.T) {
	dir := t.TempDir()

	newDB := func(t *testing.T) *DB {
		return New(WithPath(filepath.Join(dir, t.Name())),
			WithPermissions(0o600), WithEpochState(epochStateImpl{}))
	}
	check := func(t *testing.T, db *DB) {
		require.NoError(t, db.boltDB.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(shardInfoBucket)
			if b == nil {
				return errors.New("shard info bucket not found")
			}
			data := b.Get(versionKey)
			if len(data) != 8 {
				return errors.New("invalid version data")
			}
			if stored := binary.LittleEndian.Uint64(data); stored != version {
				return fmt.Errorf("invalid version: %d != %d", stored, version)
			}
			return nil
		}))
	}
	t.Run("simple", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.Init())
		check(t, db)
		require.NoError(t, db.Close())

		t.Run("reopen", func(t *testing.T) {
			require.NoError(t, db.Open(false))
			require.NoError(t, db.Init())
			check(t, db)
			require.NoError(t, db.Close())
		})
	})
	t.Run("old data", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.WriteShardID([]byte{1, 2, 3, 4}))
		require.NoError(t, db.Close())

		require.NoError(t, db.Open(false))
		require.NoError(t, db.Init())
		check(t, db)
		require.NoError(t, db.Close())
	})
	t.Run("invalid version", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
			return updateVersion(tx, version+1)
		}))
		require.NoError(t, db.Close())

		require.NoError(t, db.Open(false))
		require.Error(t, db.Init())
		require.NoError(t, db.Close())

		t.Run("reset", func(t *testing.T) {
			require.NoError(t, db.Open(false))
			require.NoError(t, db.Reset())
			check(t, db)
			require.NoError(t, db.Close())
		})
	})
}
