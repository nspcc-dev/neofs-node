package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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
		moved, err := e.OptimizeShardLocation(context.Background(), addr, addr.Object(), []string{source.ID().String()})
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
		moved, err := e.OptimizeShardLocation(context.Background(), addr, addr.Object(), []string{target.ID().String(), source.ID().String()})
		require.NoError(t, err)
		require.False(t, moved)
	})

	t.Run("does not move only target copy", func(t *testing.T) {
		e := newEngine(t)
		obj := generateObjectWithCID(cidtest.ID())
		addr := obj.Address()
		target := e.sortedShards(addr.Object())[0]

		require.NoError(t, target.Put(obj, nil))
		moved, err := e.OptimizeShardLocation(context.Background(), addr, addr.Object(), []string{target.ID().String()})
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
		moved, err := e.OptimizeShardLocation(context.Background(), addr, addr.Object(), []string{source.ID().String()})
		require.False(t, moved)
		require.ErrorIs(t, err, shard.ErrReadOnlyMode)

		got, err := source.Get(addr, false)
		require.NoError(t, err)
		require.Equal(t, obj, got)
	})

	for _, tc := range []struct {
		name     string
		onTarget bool
		onSource bool
		readOnly bool
	}{
		{name: "already optimal", onTarget: true},
		{name: "misplaced", onSource: true},
		{name: "duplicates", onTarget: true, onSource: true},
		{name: "read-only target", onSource: true, readOnly: true},
	} {
		t.Run("EC part/"+tc.name, func(t *testing.T) {
			ctx := context.Background()
			e := newEngine(t, shard.WithGCRemoverSleepInterval(100*time.Millisecond))
			parent := generateObjectWithCID(cidtest.ID())
			parentID := parent.GetID()
			shards := e.sortedShards(parentID)
			target, source := shards[0], shards[1]
			pi := iec.PartInfo{RuleIndex: 0, Index: 0}
			signer := neofscryptotest.Signer()
			var part object.Object
			// Ensure that ordering by the part ID would select the wrong shard.
			for i := range 100 {
				var err error
				part, err = iec.FormObjectForECPart(signer, *parent, []byte{byte(i)}, pi)
				require.NoError(t, err)
				if e.sortedShards(part.GetID())[0].ID().String() != target.ID().String() {
					break
				}
			}
			require.NotEqual(t, target.ID().String(), e.sortedShards(part.GetID())[0].ID().String())
			addr := part.Address()
			var shardIDs []string
			if tc.onTarget {
				// Exercise the same placement path as the initial upload.
				require.NoError(t, e.Put(ctx, &part, nil))
				_, err := target.Get(addr, false)
				require.NoError(t, err)
				shardIDs = append(shardIDs, target.ID().String())
			}
			if tc.onSource {
				require.NoError(t, source.Put(&part, nil))
				shardIDs = append(shardIDs, source.ID().String())
			}
			if tc.readOnly {
				require.NoError(t, target.SetMode(mode.ReadOnly))
			}

			moved, err := e.OptimizeShardLocation(ctx, addr, parentID, shardIDs)
			if tc.readOnly {
				require.ErrorIs(t, err, shard.ErrReadOnlyMode)
				require.False(t, moved)
				got, err := source.Get(addr, false)
				require.NoError(t, err)
				require.Equal(t, &part, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, !tc.onTarget, moved)
				require.Eventually(t, func() bool {
					_, err := source.Get(addr, false)
					return errors.Is(err, apistatus.ErrObjectNotFound)
				}, time.Second, 10*time.Millisecond)
				moved, err = e.OptimizeShardLocation(ctx, addr, parentID, []string{target.ID().String()})
				require.NoError(t, err)
				require.False(t, moved)
			}

			hdr, rc, err := e.GetECPart(ctx, addr.Container(), parentID, pi, false)
			require.NoError(t, err)
			assertGetECPartOK(t, part, hdr, rc)
			require.NoError(t, rc.Close())

			hdr, err = e.HeadECPart(ctx, addr.Container(), parentID, pi)
			require.NoError(t, err)
			require.Equal(t, part.CutPayload(), &hdr)

			rangeHdr, pldLen, rc, err := e.GetECPartRange(ctx, addr.Container(), parentID, pi, blobstor.NewPayloadRange(0, 1), true)
			require.NoError(t, err)
			require.Equal(t, part.CutPayload(), rangeHdr)
			assertGetECPartRangeOK(t, part, 0, 1, pldLen, rc)
		})
	}

	t.Run("fails when source is missing", func(t *testing.T) {
		e := newEngine(t)
		obj := generateObjectWithCID(cidtest.ID())
		addr := obj.Address()
		target := e.sortedShards(addr.Object())[0]

		moved, err := e.OptimizeShardLocation(context.Background(), addr, addr.Object(), []string{"missing"})
		require.False(t, moved)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		_, err = target.Get(addr, false)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	})
}
