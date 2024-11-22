package shard_test

import (
	"crypto/rand"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestWriteCacheObjectLoss(t *testing.T) {
	const (
		smallSize = 1024
		objCount  = 100
	)

	objects := make([]*objectSDK.Object, objCount)
	for i := range objects {
		size := smallSize / 2
		data := make([]byte, size)
		_, _ = rand.Read(data)

		objects[i] = generateObjectWithPayload(cidtest.ID(), data)
	}

	dir := t.TempDir()
	wcOpts := []writecache.Option{
		writecache.WithSmallObjectSize(smallSize),
		writecache.WithMaxObjectSize(smallSize * 2)}

	sh := newCustomShard(t, dir, true, wcOpts, nil)

	var putPrm shard.PutPrm

	for i := range objects {
		putPrm.SetObject(objects[i])
		_, err := sh.Put(putPrm)
		require.NoError(t, err)
	}
	require.NoError(t, sh.Close())

	sh = newCustomShard(t, dir, true, wcOpts, nil)
	defer releaseShard(sh, t)

	for i := range objects {
		_, err := sh.Get(object.AddressOf(objects[i]), false)
		require.NoError(t, err, i)
	}
}
