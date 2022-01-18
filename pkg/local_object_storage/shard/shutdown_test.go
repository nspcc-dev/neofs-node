package shard_test

import (
	"math/rand"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestWriteCacheObjectLoss(t *testing.T) {
	const (
		smallSize = 1024
		objCount  = 100
	)

	objects := make([]*object.Object, objCount)
	for i := range objects {
		size := smallSize
		//if i%2 == 0 {
		size = smallSize / 2
		//}
		data := make([]byte, size)
		rand.Read(data)

		objects[i] = generateRawObjectWithPayload(cidtest.ID(), data).Object()
	}

	dir := t.TempDir()
	wcOpts := []writecache.Option{
		writecache.WithSmallObjectSize(smallSize),
		writecache.WithMaxObjectSize(smallSize * 2)}

	sh := newCustomShard(t, dir, true, wcOpts...)

	for i := range objects {
		_, err := sh.Put(new(shard.PutPrm).WithObject(objects[i]))
		require.NoError(t, err)
	}
	require.NoError(t, sh.Close())

	sh = newCustomShard(t, dir, true, wcOpts...)
	defer releaseShard(sh, t)

	for i := range objects {
		_, err := sh.Get(new(shard.GetPrm).WithAddress(objects[i].Address()))
		require.NoError(t, err, i)
	}
}
