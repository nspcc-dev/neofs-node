package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_OptimizeShardLocation(t *testing.T) {
	newEngine := func(t *testing.T, opts ...shard.Option) *StorageEngine {
		var e *StorageEngine
		if len(opts) == 0 {
			e = testNewEngineWithShards(testNewShard(t, 1), testNewShard(t, 2))
		} else {
			e = testEngineFromShardOpts(t, 2, opts)
		}
		t.Cleanup(func() { _ = e.Close() })
		return e
	}

	t.Run("moves to preferred shard", func(t *testing.T) {
		e := newEngine(t, shard.WithGCRemoverSleepInterval(100*time.Millisecond))
		obj := generateObjectWithCID(cidtest.ID())
		addr := obj.Address()
		shards := e.sortedShards(addr.Object())
		target, source := shards[0], shards[1]

		require.NoError(t, source.Put(obj, nil))
		moved, err := e.OptimizeShardLocation(context.Background(), addr, []string{source.ID().String()})
		require.NoError(t, err)
		require.True(t, moved)

		got, err := target.Get(addr, false)
		require.NoError(t, err)
		require.Equal(t, obj, got)

		require.Eventually(t, func() bool {
			_, err := source.Get(addr, false)
			return errors.Is(err, apistatus.ErrObjectNotFound)
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("does not move when target already contains object", func(t *testing.T) {
		e := newEngine(t)
		obj := generateObjectWithCID(cidtest.ID())
		addr := obj.Address()
		shards := e.sortedShards(addr.Object())
		target, source := shards[0], shards[1]

		require.NoError(t, target.Put(obj, nil))
		require.NoError(t, source.Put(obj, nil))
		moved, err := e.OptimizeShardLocation(context.Background(), addr, []string{target.ID().String(), source.ID().String()})
		require.NoError(t, err)
		require.False(t, moved)
	})

	t.Run("does not move only target copy", func(t *testing.T) {
		e := newEngine(t)
		obj := generateObjectWithCID(cidtest.ID())
		addr := obj.Address()
		target := e.sortedShards(addr.Object())[0]

		require.NoError(t, target.Put(obj, nil))
		moved, err := e.OptimizeShardLocation(context.Background(), addr, []string{target.ID().String()})
		require.NoError(t, err)
		require.False(t, moved)
	})

	t.Run("keeps source when target is read-only", func(t *testing.T) {
		e := newEngine(t)
		obj := generateObjectWithCID(cidtest.ID())
		addr := obj.Address()
		shards := e.sortedShards(addr.Object())
		target, source := shards[0], shards[1]

		require.NoError(t, source.Put(obj, nil))
		require.NoError(t, target.SetMode(mode.ReadOnly))
		moved, err := e.OptimizeShardLocation(context.Background(), addr, []string{source.ID().String()})
		require.False(t, moved)
		require.ErrorIs(t, err, shard.ErrReadOnlyMode)

		got, err := source.Get(addr, false)
		require.NoError(t, err)
		require.Equal(t, obj, got)
	})

	t.Run("fails when source is missing", func(t *testing.T) {
		e := newEngine(t)
		obj := generateObjectWithCID(cidtest.ID())
		addr := obj.Address()
		target := e.sortedShards(addr.Object())[0]

		moved, err := e.OptimizeShardLocation(context.Background(), addr, []string{"missing"})
		require.False(t, moved)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		_, err = target.Get(addr, false)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	})
}
