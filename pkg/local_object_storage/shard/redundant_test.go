package shard

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestRedundantQueue(t *testing.T) {
	t.Run("deduplication and snapshots", func(t *testing.T) {
		var q redundantQueue
		cnr, id := cidtest.ID(), oidtest.ID()
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() { require.NoError(t, q.add(cnr, []oid.ID{id, id})) })
		}
		wg.Wait()
		require.Len(t, q.pending, 1)
		require.Empty(t, q.batch(0))
		require.Empty(t, q.batch(-1))
		bins := q.batch(1)
		require.Equal(t, []meta.TrashBin{{Container: cnr, Objects: []oid.ID{id}}}, bins)
		require.NoError(t, q.add(cnr, []oid.ID{id}))
		// A snapshot does not consume work or allow duplicate in-flight tasks.
		require.Equal(t, bins, q.batch(1))
		q.remove(cnr, []oid.ID{id})
		require.Empty(t, q.pending)
	})

	t.Run("removal and batch limit", func(t *testing.T) {
		var q redundantQueue
		cnr, other := cidtest.ID(), cidtest.ID()
		id, keep := oidtest.ID(), oidtest.ID()
		require.NoError(t, q.add(cnr, []oid.ID{id, keep}))
		require.NoError(t, q.add(other, []oid.ID{id}))
		q.remove(cnr, []oid.ID{id, id, oidtest.ID()})
		require.Len(t, q.pending, 2)
		require.Contains(t, q.pending, oid.NewAddress(cnr, keep))
		require.Contains(t, q.pending, oid.NewAddress(other, id))
		require.NoError(t, q.add(cnr, []oid.ID{id}))
		require.Len(t, q.pending, 3)
		var count int
		for _, bin := range q.batch(2) {
			count += len(bin.Objects)
		}
		require.Equal(t, 2, count)
	})

	t.Run("capacity", func(t *testing.T) {
		var q redundantQueue
		cnr := cidtest.ID()
		ids := make([]oid.ID, redundantQueueLimit)
		for i := range ids {
			ids[i] = oidtest.ID()
		}
		require.NoError(t, q.add(cnr, ids))
		require.Len(t, q.batch(1), 1)
		require.NoError(t, q.add(cnr, ids[:1]))
		require.ErrorIs(t, q.add(cnr, []oid.ID{oidtest.ID()}), errRedundantQueueFull)
	})

	t.Run("closed", func(t *testing.T) {
		var q redundantQueue
		cnr, ids := cidtest.ID(), []oid.ID{oidtest.ID()}
		q.setClosed(true)
		require.ErrorIs(t, q.add(cnr, ids), errRedundantQueueClosed)
		q.setClosed(false)
		require.NoError(t, q.add(cnr, ids))
	})
}

func newRedundantTestShard(t *testing.T, dir string, wc bool, options ...Option) *Shard {
	t.Helper()
	opts := []Option{
		WithLogger(zaptest.NewLogger(t)),
		WithBlobstor(fstree.New(fstree.WithPath(filepath.Join(dir, "fstree")))),
		WithMetaBaseOptions(meta.WithPath(filepath.Join(dir, "meta")),
			meta.WithEpochState(epochState{}), meta.WithMaxBatchDelay(time.Microsecond)),
		WithWriteCache(wc),
		WithWriteCacheOptions(writecache.WithPath(filepath.Join(dir, "wc"))),
		WithGCRemoverSleepInterval(time.Hour),
	}
	sh := New(append(opts, options...)...)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())
	t.Cleanup(func() { require.NoError(t, sh.Close()) })
	return sh
}

func TestShard_Redundant(t *testing.T) {
	queued := func(t *testing.T, wc bool, opts ...Option) (*Shard, *object.Object) {
		t.Helper()
		sh := newRedundantTestShard(t, t.TempDir(), wc, opts...)
		obj := generateTypedObject(cidtest.ID(), object.TypeRegular)
		obj.SetPayload([]byte("payload"))
		obj.SetPayloadSize(uint64(len(obj.Payload())))
		require.NoError(t, sh.Put(obj, nil))
		require.NoError(t, sh.MarkGarbage(obj.GetContainerID(), []oid.ID{obj.GetID()}, meta.GarbageMarkRedundant))
		return sh, obj
	}

	t.Run("GC", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			wc   bool
		}{{"blobstor", false}, {"writecache", true}} {
			t.Run(tc.name, func(t *testing.T) {
				sh, obj := queued(t, tc.wc)
				bins, err := sh.metaBase.GetGarbage(10)
				require.NoError(t, err)
				require.Empty(t, bins)
				before, err := sh.metaBase.ObjectCounters()
				require.NoError(t, err)
				require.Zero(t, before.GC)
				require.Equal(t, obj.PayloadSize(), before.Payload)
				_, err = sh.Get(obj.Address(), false)
				require.NoError(t, err)
				sh.removeGarbage()
				_, err = sh.Get(obj.Address(), false)
				require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
				require.Empty(t, sh.redundant.pending)
				after, err := sh.metaBase.ObjectCounters()
				require.NoError(t, err)
				require.Zero(t, after)
			})
		}
		t.Run("default mark overrides readability", func(t *testing.T) {
			sh, obj := queued(t, true)
			require.NoError(t, sh.MarkGarbage(obj.GetContainerID(), []oid.ID{obj.GetID()}, meta.GarbageMarkDefault))
			_, err := sh.Get(obj.Address(), false)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
			sh.removeGarbage()
			require.Empty(t, sh.redundant.pending)
		})
	})

	t.Run("flush", func(t *testing.T) {
		for _, batchSize := range []int{0, 1} {
			name := "disabled GC"
			if batchSize != 0 {
				name = "small GC batch"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				sh := newRedundantTestShard(t, dir, false, WithRemoverBatchSize(batchSize))
				want := make(map[cid.ID][]oid.ID)
				for range 3 {
					cnr := cidtest.ID()
					for range 4 {
						obj := generateTypedObject(cnr, object.TypeRegular)
						obj.SetPayload([]byte("payload"))
						obj.SetPayloadSize(uint64(len(obj.Payload())))
						require.NoError(t, sh.Put(obj, nil))
						want[cnr] = append(want[cnr], obj.GetID())
					}
					require.NoError(t, sh.MarkGarbage(cnr, want[cnr], meta.GarbageMarkRedundant))
				}
				// A failed flush retains every container, independent of GC batch size.
				require.NoError(t, sh.metaBase.SetMode(mode.ReadOnly))
				sh.m.Lock()
				err := sh.flushRedundant()
				sh.m.Unlock()
				require.NoError(t, sh.metaBase.SetMode(mode.ReadWrite))
				require.ErrorIs(t, err, meta.ErrReadOnlyMode)
				require.Len(t, sh.redundant.pending, 12)
				require.NoError(t, sh.Close())
				require.Empty(t, sh.redundant.pending)
				require.ErrorIs(t, sh.MarkGarbage(cidtest.ID(), nil, meta.GarbageMarkRedundant), errRedundantQueueClosed)

				reopened := newRedundantTestShard(t, dir, false)
				bins, err := reopened.metaBase.GetGarbage(100)
				require.NoError(t, err)
				require.Len(t, bins, len(want))
				for _, bin := range bins {
					require.ElementsMatch(t, want[bin.Container], bin.Objects)
					for _, id := range bin.Objects {
						_, err = reopened.Get(oid.NewAddress(bin.Container, id), false)
						require.NoError(t, err)
					}
				}
				counters, err := reopened.metaBase.ObjectCounters()
				require.NoError(t, err)
				require.EqualValues(t, 12, counters.GC)
				require.Zero(t, counters.Payload)
				reopened.removeGarbage()
				counters, err = reopened.metaBase.ObjectCounters()
				require.NoError(t, err)
				require.Zero(t, counters)
			})
		}
	})

	t.Run("mode switch", func(t *testing.T) {
		sh, obj := queued(t, false)
		require.NoError(t, sh.SetMode(mode.ReadOnly))
		require.Empty(t, sh.redundant.pending)
		bins, err := sh.metaBase.GetGarbage(10)
		require.NoError(t, err)
		require.Len(t, bins, 1)
		require.ErrorIs(t, sh.MarkGarbage(obj.GetContainerID(), []oid.ID{obj.GetID()}, meta.GarbageMarkRedundant), ErrReadOnlyMode)
		require.NoError(t, sh.SetMode(mode.ReadWrite))
		sh.removeGarbage()
		_, err = sh.Get(obj.Address(), false)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	})

	t.Run("failed operations retain tasks", func(t *testing.T) {
		sh, obj := queued(t, false)
		_, err := sh.ReviveObject(obj.Address())
		require.ErrorIs(t, err, meta.ErrObjectWasNotRemoved)
		require.Len(t, sh.redundant.pending, 1)
		require.NoError(t, sh.metaBase.SetMode(mode.ReadOnly))
		sh.removeGarbage()
		require.Len(t, sh.redundant.pending, 1)
		err = sh.Delete(obj.GetContainerID(), []oid.ID{obj.GetID()})
		require.ErrorIs(t, err, meta.ErrReadOnlyMode)
		require.Len(t, sh.redundant.pending, 1)
		// Flush failure must not prevent switching to a safer mode.
		require.NoError(t, sh.SetMode(mode.ReadOnly))
		require.Len(t, sh.redundant.pending, 1)
		require.NoError(t, sh.SetMode(mode.ReadWrite))
		sh.removeGarbage()
		require.Empty(t, sh.redundant.pending)
	})

	t.Run("close reports flush failure", func(t *testing.T) {
		sh, _ := queued(t, false)
		require.NoError(t, sh.metaBase.SetMode(mode.ReadOnly))
		require.ErrorIs(t, sh.Close(), meta.ErrReadOnlyMode)
		require.Len(t, sh.redundant.pending, 1)
		_, err := sh.metaBase.ObjectCounters()
		require.Error(t, err)
		// Reopening allows the retained work to be persisted on the next close.
		require.NoError(t, sh.Open())
		require.NoError(t, sh.SetMode(mode.ReadWrite))
		require.NoError(t, sh.Init())
		require.NoError(t, sh.Close())
		require.Empty(t, sh.redundant.pending)
	})

	t.Run("parts", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			version int
			wc      bool
			nested  bool
		}{
			{name: "split v1", version: 1},
			{name: "split v2", version: 2, wc: true},
			{name: "nested v1", version: 1, wc: true, nested: true},
			{name: "nested v2", version: 2, nested: true},
			{name: "EC", wc: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, action := range []string{"GC", "delete"} {
					t.Run(action, func(t *testing.T) {
						sh := newRedundantTestShard(t, t.TempDir(), tc.wc)
						parent := generateTypedObject(cidtest.ID(), object.TypeRegular)
						var parts []*object.Object
						if tc.version == 0 {
							signer := neofscryptotest.Signer()
							for i := range 3 {
								part, err := iec.FormObjectForECPart(signer, *parent, []byte("part"), iec.PartInfo{RuleIndex: 0, Index: i})
								require.NoError(t, err)
								parts = append(parts, &part)
							}
						} else {
							addParts := func(parent *object.Object) *object.Object {
								first := generateTypedObject(parent.GetContainerID(), object.TypeRegular)
								first.SetPayload([]byte("part"))
								first.SetPayloadSize(uint64(len(first.Payload())))
								first.SetParent(parent)
								first.SetParentID(parent.GetID())
								link := generateTypedObject(parent.GetContainerID(), object.TypeLink)
								link.SetPayload(nil)
								link.SetPayloadSize(0)
								link.SetParent(parent)
								link.SetParentID(parent.GetID())
								if tc.version == 1 {
									splitID := object.NewSplitID()
									first.SetSplitID(splitID)
									link.SetSplitID(splitID)
									link.SetType(object.TypeRegular)
									link.SetChildren(first.GetID())
								} else {
									link.SetFirstID(first.GetID())
								}
								parts = append(parts, first, link)
								return first
							}
							first := addParts(parent)
							if tc.nested {
								addParts(first)
							}
						}
						ids := []oid.ID{parent.GetID()}
						for _, part := range parts {
							require.NoError(t, sh.Put(part, nil))
							if action == "delete" {
								ids = append(ids, part.GetID())
							}
						}
						if tc.nested && action == "GC" {
							// Overlapping parent/child tasks, with the child first.
							ids = []oid.ID{parts[0].GetID(), parent.GetID()}
						}
						require.NoError(t, sh.MarkGarbage(parent.GetContainerID(), ids, meta.GarbageMarkRedundant))
						if action == "delete" {
							require.NoError(t, sh.Delete(parent.GetContainerID(), []oid.ID{parent.GetID()}))
						} else {
							sh.removeGarbage()
						}
						require.Empty(t, sh.redundant.pending)
						for _, part := range parts {
							_, err := sh.Get(part.Address(), false)
							require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
							_, err = sh.blobStor.Get(part.Address())
							require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
							if tc.wc {
								_, err = sh.writeCache.Get(part.Address())
								require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
							}
						}
						counters, err := sh.metaBase.ObjectCounters()
						require.NoError(t, err)
						require.Zero(t, counters)
						if action == "delete" {
							for _, part := range parts {
								require.NoError(t, sh.Put(part, nil))
							}
							sh.removeGarbage()
							for _, part := range parts {
								// Nested parts are parents too; read their physical copies.
								_, err := sh.Get(part.Address(), true)
								require.NoError(t, err)
							}
						}
					})
				}
			})
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		for _, action := range []string{"delete", "revive"} {
			t.Run(action, func(t *testing.T) {
				sh, obj := queued(t, false)
				ids := []oid.ID{obj.GetID()}
				if action == "revive" {
					require.NoError(t, sh.MarkGarbage(obj.GetContainerID(), ids, meta.GarbageMarkDefault))
				}
				if action == "delete" {
					require.NoError(t, sh.Delete(obj.GetContainerID(), ids))
				} else {
					_, err := sh.ReviveObject(obj.Address())
					require.NoError(t, err)
				}
				require.Empty(t, sh.redundant.pending)
				if action == "delete" {
					require.NoError(t, sh.Put(obj, nil))
				}
				sh.removeGarbage()
				_, err := sh.Get(obj.Address(), false)
				require.NoError(t, err)
			})
		}
	})

	t.Run("missing container", func(t *testing.T) {
		sh, obj := queued(t, true)
		cnr, ids := obj.GetContainerID(), []oid.ID{obj.GetID()}
		require.NoError(t, sh.Delete(cnr, ids))
		require.Empty(t, sh.redundant.pending)
		require.NoError(t, sh.metaBase.DeleteContainer(cnr))
		// A stale policer listing may enqueue an object after container removal.
		require.NoError(t, sh.MarkGarbage(cnr, ids, meta.GarbageMarkRedundant))
		require.NoError(t, sh.Delete(cnr, ids))
		require.Empty(t, sh.redundant.pending)
		require.NoError(t, sh.MarkGarbage(cnr, ids, meta.GarbageMarkRedundant))
		require.NotPanics(t, sh.removeGarbage)
		require.Empty(t, sh.redundant.pending)
		require.NoError(t, sh.Delete(cnr, ids))
	})

	t.Run("concurrent close", func(t *testing.T) {
		sh, obj := queued(t, true, WithGCRemoverSleepInterval(time.Millisecond), WithRemoverBatchSize(1))
		started, done := make(chan struct{}), make(chan error, 1)
		go func() {
			close(started)
			for {
				if err := sh.MarkGarbage(obj.GetContainerID(), []oid.ID{obj.GetID()}, meta.GarbageMarkRedundant); err != nil {
					done <- err
					return
				}
			}
		}()
		<-started
		require.NoError(t, sh.Close())
		require.ErrorIs(t, <-done, errRedundantQueueClosed)
		require.Empty(t, sh.redundant.pending)
	})
}
