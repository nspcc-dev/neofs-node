package writecache

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestMigrateFromBolt(t *testing.T) {
	c, s := newCache(t)

	wc := c.(*cache)
	path := filepath.Join(wc.path, dbName)

	require.NoError(t, wc.Close())

	t.Run("ok, no database", func(t *testing.T) {
		require.NoError(t, wc.migrate())
	})

	db, err := bbolt.Open(path, os.ModePerm, &bbolt.Options{
		NoFreelistSync: true,
		ReadOnly:       false,
		Timeout:        time.Second,
	})
	require.NoError(t, err)

	t.Run("couldn't open database", func(t *testing.T) {
		err := wc.migrate()
		require.Error(t, err)
	})

	require.NoError(t, db.Close())
	t.Run("no default bucket", func(t *testing.T) {
		err := wc.migrate()
		require.Error(t, err)
	})

	db, err = bbolt.Open(path, os.ModePerm, &bbolt.Options{
		NoFreelistSync: true,
		ReadOnly:       false,
		Timeout:        time.Second,
	})
	require.NoError(t, err)

	obj := objecttest.Object()

	var addr oid.Address
	addr.SetObject(obj.GetID())
	addr.SetContainer(obj.GetContainerID())

	require.NoError(t, db.Batch(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(defaultBucket)
		require.NoError(t, err)

		require.NoError(t, b.Put([]byte(addr.String()), obj.Marshal()))

		return nil
	}))

	t.Run("migrate object", func(t *testing.T) {
		require.NoError(t, db.Close())
		require.NoError(t, wc.migrate())

		_, err := wc.Get(addr)
		require.Error(t, err, apistatus.ObjectNotFound{})

		bObject, err := s.Get(addr)
		require.NoError(t, err)
		require.Equal(t, obj.GetID(), bObject.GetID())
		require.Equal(t, obj.GetContainerID(), bObject.GetContainerID())
		require.Equal(t, obj.Marshal(), bObject.Marshal())

		_, err = os.Stat(path)
		require.Error(t, err, os.ErrNotExist)
	})
}
