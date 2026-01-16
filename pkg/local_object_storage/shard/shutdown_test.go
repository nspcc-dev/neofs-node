package shard_test

import (
	"crypto/rand"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestWriteCacheObjectLoss(t *testing.T) {
	const (
		objectSize = 1024
		objCount   = 100
	)

	objects := make([]*object.Object, objCount)
	for i := range objects {
		data := make([]byte, objectSize)
		_, _ = rand.Read(data)

		objects[i] = generateObjectWithPayload(cidtest.ID(), data)
	}

	dir := t.TempDir()
	wcOpts := []writecache.Option{}

	sh := newCustomShard(t, dir, true, wcOpts)

	for i := range objects {
		err := sh.Put(objects[i], nil)
		require.NoError(t, err)
	}
	require.NoError(t, sh.Close())

	sh = newCustomShard(t, dir, true, wcOpts)
	defer releaseShard(sh, t)

	for i := range objects {
		_, err := sh.Get(objects[i].Address(), false)
		require.NoError(t, err, i)
	}
}
