package boltdb

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

var config = strings.NewReader(`
storage:
  test_bucket:
    bucket: boltdb
    path: ./temp/storage/test_bucket
    perm: 0777
`)

func TestBucket(t *testing.T) {
	file, err := ioutil.TempFile("", "test_bolt_db")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	v := viper.New()
	require.NoError(t, v.ReadConfig(config))

	// -- //
	_, err = NewOptions("storage.test_bucket", v)
	require.EqualError(t, err, errEmptyPath.Error())

	v.SetDefault("storage.test_bucket.path", file.Name())
	v.SetDefault("storage.test_bucket.timeout", time.Millisecond*100)
	// -- //

	opts, err := NewOptions("storage.test_bucket", v)
	require.NoError(t, err)

	db, err := NewBucket(&opts)
	require.NoError(t, err)

	require.NotPanics(t, func() { db.Size() })

	var (
		count    = uint64(10)
		expected = []byte("test")
	)

	for i := uint64(0); i < count; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)

		require.False(t, db.Has(key))

		val, err := db.Get(key)
		require.EqualError(t, errors.Cause(err), core.ErrNotFound.Error())
		require.Empty(t, val)

		require.NoError(t, db.Set(key, expected))

		require.True(t, db.Has(key))

		val, err = db.Get(key)
		require.NoError(t, err)
		require.Equal(t, expected, val)

		keys, err := db.List()
		require.NoError(t, err)
		require.Len(t, keys, 1)
		require.Equal(t, key, keys[0])

		require.EqualError(t, db.Iterate(nil), core.ErrNilFilterHandler.Error())

		items, err := core.ListBucketItems(db, func(_, _ []byte) bool { return true })
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Equal(t, key, items[0].Key)
		require.Equal(t, val, items[0].Val)

		require.NoError(t, db.Del(key))
		require.False(t, db.Has(key))

		val, err = db.Get(key)
		require.EqualError(t, errors.Cause(err), core.ErrNotFound.Error())
		require.Empty(t, val)
	}

	require.NoError(t, db.Close())
	require.NoError(t, os.RemoveAll(file.Name()))
}
