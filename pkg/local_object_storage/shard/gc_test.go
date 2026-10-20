package shard_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/chaintime"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type testContainerPayments struct {
	m                sync.RWMutex
	cnrs             map[cid.ID]int64
	paymentsDisabled bool
}

func (p *testContainerPayments) PaymentsDisabled() bool {
	p.m.RLock()
	defer p.m.RUnlock()

	return p.paymentsDisabled
}

func (p *testContainerPayments) UnpaidSince(id cid.ID) (int64, error) {
	p.m.RLock()
	defer p.m.RUnlock()

	if p.cnrs == nil {
		return -1, nil
	}

	return p.cnrs[id], nil
}

func TestGC_ExpiredObjectWithExpiredLock(t *testing.T) {
	const gcInterval = 200 * time.Millisecond
	var sh *shard.Shard

	epoch := &epochState{
		Value: 2,
	}
	rootPath := t.TempDir()
	opts := []shard.Option{
		shard.WithLogger(zaptest.NewLogger(t)),
		shard.WithBlobstor(fstree.New(
			fstree.WithPath(filepath.Join(rootPath, "fstree"))),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epoch),
			meta.WithMaxBatchDelay(time.Microsecond),
		),
		shard.WithExpiredObjectsCallback(func(aa []oid.Address) {
			for _, a := range aa {
				require.NoError(t, sh.Delete(a.Container(), []oid.ID{a.Object()}))
			}
		}),
		shard.WithGCRemoverSleepInterval(gcInterval),
		shard.WithContainerPayments(&testContainerPayments{}),
	}

	synctest.Test(t, func(t *testing.T) {
		sh = shard.New(opts...)
		require.NoError(t, sh.Open())
		require.NoError(t, sh.Init())

		t.Cleanup(func() {
			releaseShard(sh, t)
		})

		cnr := cidtest.ID()

		var expAttrObj, expAttrLocker object.Attribute
		expAttrObj.SetKey(object.AttributeExpirationEpoch)    //nolint:staticcheck
		expAttrLocker.SetKey(object.AttributeExpirationEpoch) //nolint:staticcheck
		expAttrObj.SetValue("1")

		obj := generateObjectWithCID(cnr)
		obj.SetAttributes(expAttrObj)
		objID := obj.GetID()

		expAttrLocker.SetValue("3")

		lock := generateObjectWithCID(cnr)
		lock.SetAttributes(expAttrLocker)
		lock.AssociateLocked(objID)

		err := sh.Put(obj, nil)
		require.NoError(t, err)

		err = sh.Put(lock, nil)
		require.NoError(t, err)

		_, err = sh.Get(obj.Address(), false)
		require.NoError(t, err)

		epoch.Value = 5
		sh.NotificationChannel() <- shard.EventNewEpoch(epoch.Value)

		time.Sleep(gcInterval)

		_, err = sh.Get(obj.Address(), false)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	})
}

func TestGC_ContainerCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sh := newCustomShard(t, t.TempDir(), false,
			nil,
			shard.WithGCRemoverSleepInterval(10*time.Millisecond),
			shard.WithLogger(zaptest.NewLogger(t)))
		defer releaseShard(sh, t)

		const numOfObjs = 10
		cID := cidtest.ID()
		oo := make([]oid.Address, 0, numOfObjs)

		for i := range numOfObjs {
			obj := generateObjectWithCID(cID)
			addAttribute(obj, fmt.Sprintf("foo%d", i), fmt.Sprintf("bar%d", i))
			if i%2 == 0 {
				addPayload(obj, 1<<5) // small
			} else {
				addPayload(obj, 1<<20) // big
			}

			err := sh.Put(obj, nil)
			require.NoError(t, err)

			oo = append(oo, obj.Address())
		}

		containers, err := sh.ListContainers()
		require.NoError(t, err)
		require.Len(t, containers, 1)

		for _, o := range oo {
			_, err = sh.Get(o, false)
			require.NoError(t, err)
		}

		require.NoError(t, sh.InhumeContainer(cID))

		time.Sleep(time.Second)

		containers, err = sh.ListContainers()
		require.NoError(t, err)
		require.Zero(t, len(containers))

		for _, o := range oo {
			_, err = sh.Get(o, false)
			require.ErrorIs(t, err, apistatus.ObjectNotFound{})
		}
	})
}

func TestExpiration(t *testing.T) {
	rootPath := t.TempDir()
	var sh *shard.Shard
	const gcInterval = 100 * time.Millisecond

	opts := []shard.Option{
		shard.WithLogger(zaptest.NewLogger(t)),
		shard.WithBlobstor(fstree.New(
			fstree.WithPath(filepath.Join(rootPath, "fstree"))),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epochState{Value: 0}),
			meta.WithMaxBatchDelay(time.Microsecond),
		),
		shard.WithExpiredObjectsCallback(
			func(aa []oid.Address) {
				for _, a := range aa {
					require.NoError(t, sh.Delete(a.Container(), []oid.ID{a.Object()}))
				}
			},
		),
		shard.WithGCRemoverSleepInterval(gcInterval),
		shard.WithContainerPayments(&testContainerPayments{}),
	}

	synctest.Test(t, func(t *testing.T) {
		sh = shard.New(opts...)
		require.NoError(t, sh.Open())
		require.NoError(t, sh.Init())

		t.Cleanup(func() {
			releaseShard(sh, t)
		})
		ch := sh.NotificationChannel()

		var expAttr object.Attribute
		expAttr.SetKey(object.AttributeExpirationEpoch) //nolint:staticcheck

		obj := generateObject()

		for i, typ := range []object.Type{object.TypeRegular, object.TypeTombstone, object.TypeLink} {
			exp := uint64(i * 10)

			expAttr.SetValue(strconv.FormatUint(exp, 10))
			obj.SetAttributes(expAttr)
			obj.SetType(typ)
			if typ == object.TypeTombstone {
				obj.AssociateDeleted(oidtest.ID())
			}
			require.NoError(t, obj.SetIDWithSignature(neofscryptotest.Signer()))

			err := sh.Put(obj, nil)
			require.NoError(t, err)

			_, err = sh.Get(obj.Address(), false)
			require.NoError(t, err)

			ch <- shard.EventNewEpoch(exp + 1)

			time.Sleep(gcInterval * 2)
			_, err = sh.Get(obj.Address(), false)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound, fmt.Sprintf("expiration should lead to object removal for %s", typ))
		}
	})
}

func TestExpirationChainTime(t *testing.T) {
	for _, tc := range []struct {
		name             string
		typ              object.Type
		locked           bool
		restart          bool
		expiredEpochLock bool
		changeMode       bool
	}{
		{name: "regular", typ: object.TypeRegular},
		{name: "tombstone", typ: object.TypeTombstone},
		{name: "lock", typ: object.TypeLock},
		{name: "link", typ: object.TypeLink},
		{name: "locked regular", typ: object.TypeRegular, locked: true},
		{name: "restart before first header notification", typ: object.TypeRegular, restart: true},
		{name: "expired epoch lock without notification", typ: object.TypeRegular, expiredEpochLock: true},
		{name: "mode change in expiration callback", typ: object.TypeRegular, changeMode: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const gcInterval = 100 * time.Millisecond
				var clock chaintime.AtomicChainTimeProvider
				clock.Set(99000)
				root := t.TempDir()
				var sh *shard.Shard
				opts := []shard.Option{
					shard.WithBlobstor(fstree.New(fstree.WithPath(filepath.Join(root, "fstree")))),
					shard.WithMetaBaseOptions(
						meta.WithPath(filepath.Join(root, "meta")),
						meta.WithEpochState(epochState{Value: 3}),
						meta.WithMaxBatchDelay(time.Microsecond),
						meta.WithTimeSource(clock.Now),
					),
					shard.WithExpiredObjectsCallback(func(aa []oid.Address) {
						for _, a := range aa {
							require.NoError(t, sh.Delete(a.Container(), []oid.ID{a.Object()}))
						}
						if tc.changeMode {
							require.NoError(t, sh.SetMode(mode.ReadOnly))
						}
					}),
					shard.WithGCRemoverSleepInterval(gcInterval),
					shard.WithContainerPayments(&testContainerPayments{}),
				}
				sh = shard.New(opts...)
				require.NoError(t, sh.Open())
				require.NoError(t, sh.Init())
				t.Cleanup(func() { releaseShard(sh, t) })

				obj := generateObject()
				obj.SetType(tc.typ)
				if tc.typ == object.TypeLock || tc.typ == object.TypeTombstone {
					obj.AssociateObject(oidtest.ID())
				}
				require.NoError(t, obj.SetExpirationTime(time.Unix(100, 0)))
				require.NoError(t, sh.Put(obj, nil))
				if tc.locked || tc.expiredEpochLock {
					lock := generateObjectWithCID(obj.GetContainerID())
					lock.AssociateLocked(obj.GetID())
					if tc.expiredEpochLock {
						lock.SetExpirationEpoch(1) //nolint:staticcheck // Test an expired epoch-based LOCK before GC gets an epoch notification.
					} else {
						require.NoError(t, lock.SetExpirationTime(time.Unix(105, 0)))
					}
					require.NoError(t, sh.Put(lock, nil))
				}
				if tc.restart {
					require.NoError(t, sh.Close())
					clock.Set(0)
					require.Greater(t, clock.Now().Unix(), int64(100)) // The shared provider retains its wall-clock fallback.
					// Like initBasics, seed time from the latest FS header before
					// starting storage, without waiting for header notifications.
					clock.Set(99000)
					sh = shard.New(opts...)
					require.NoError(t, sh.Open())
					require.NoError(t, sh.Init())
					time.Sleep(gcInterval * 2)
					_, err := sh.Get(obj.Address(), true)
					require.NoError(t, err, "GC must use the seeded chain time after restart")
				}
				// Run GC before expiration, then advance only chain time, never epoch.
				time.Sleep(gcInterval * 2)
				clock.Set(100999)
				time.Sleep(gcInterval * 2)
				_, err := sh.Get(obj.Address(), false)
				require.NoError(t, err)
				clock.Set(101000)
				time.Sleep(gcInterval * 2)
				if tc.locked {
					_, err = sh.Get(obj.Address(), false)
					require.NoError(t, err)
					clock.Set(105999)
					time.Sleep(gcInterval * 2)
					_, err = sh.Get(obj.Address(), false)
					require.NoError(t, err)
					clock.Set(106000)
					time.Sleep(gcInterval * 2)
				}
				_, err = sh.Get(obj.Address(), false)
				require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
			})
		})
	}
}

func TestContainerPayments(t *testing.T) {
	var (
		rootPath = t.TempDir()
		sh       *shard.Shard
		p        testContainerPayments
		log, lb  = testutil.NewBufferedLogger(t, zap.DebugLevel)
	)

	opts := []shard.Option{
		shard.WithLogger(log),
		shard.WithBlobstor(fstree.New(fstree.WithPath(filepath.Join(rootPath, "fstree")))),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epochState{Value: 0}),
			meta.WithMaxBatchDelay(time.Microsecond),
		),
		shard.WithExpiredObjectsCallback(
			func(aa []oid.Address) {
				for _, a := range aa {
					require.NoError(t, sh.Delete(a.Container(), []oid.ID{a.Object()}))
				}
			},
		),
		shard.WithGCRemoverSleepInterval(100 * time.Millisecond),
		shard.WithContainerPayments(&p),
	}

	sh = shard.New(opts...)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	t.Cleanup(func() {
		releaseShard(sh, t)
	})
	ch := sh.NotificationChannel()
	obj := generateObject()

	err := sh.Put(obj, nil)
	require.NoError(t, err)

	const unpaidWarningMsg = "unpaid container has been marked for removal"

	t.Run("paid container", func(t *testing.T) {
		err = sh.Put(obj, nil)
		require.NoError(t, err)

		ch <- shard.EventNewEpoch(1)
		ch <- shard.EventNewEpoch(1)
		ch <- shard.EventNewEpoch(1)
		ch <- shard.EventNewEpoch(1) // wait for epoch handling to finish at least once, its buffer is 1

		lb.AssertNotContainsMsg(zap.DebugLevel, unpaidWarningMsg)

		_, err = sh.Get(obj.Address(), false)
		require.NoError(t, err)
	})

	t.Run("unpaid container", func(t *testing.T) {
		const currEpoch = 100
		cID := obj.GetContainerID()
		unpaidSince := int64(currEpoch - 3)

		p.m.Lock()
		p.cnrs = map[cid.ID]int64{
			cID: unpaidSince,
		}
		p.m.Unlock()

		ch <- shard.EventNewEpoch(currEpoch)
		ch <- shard.EventNewEpoch(currEpoch)
		ch <- shard.EventNewEpoch(currEpoch)
		ch <- shard.EventNewEpoch(currEpoch) // wait for epoch handling to finish at least once, its buffer is 1

		expLog := testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: unpaidWarningMsg,
			Fields: map[string]any{
				"epoch":       json.Number(strconv.FormatInt(currEpoch, 10)),
				"cID":         cID.String(),
				"shard_id":    sh.ID().String(),
				"unpaidSince": json.Number(strconv.FormatInt(unpaidSince, 10)),
			},
		}
		lb.AssertContains(expLog)

		_, err = sh.Get(obj.Address(), false)
		require.ErrorIsf(t, err, apistatus.ErrObjectNotFound, "object was not removed")
	})

	t.Run("payments disabled", func(t *testing.T) {
		const currEpoch = 100

		p.m.Lock()
		p.paymentsDisabled = true
		p.m.Unlock()

		for range 4 {
			ch <- shard.EventNewEpoch(currEpoch) // wait for epoch handling to finish at least once, its buffer is 1
		}

		expLog := testutil.LogEntry{
			Level:   zap.DebugLevel,
			Message: "payments system is disabled, skipping container payments check",
			Fields: map[string]any{
				"epoch":    json.Number(strconv.FormatInt(currEpoch, 10)),
				"shard_id": sh.ID().String(),
			},
		}
		lb.AssertContains(expLog)
	})
}
