package shard_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type testContainerPayments struct {
	m    sync.RWMutex
	cnrs map[cid.ID]int64
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
		),
		shard.WithExpiredObjectsCallback(func(aa []oid.Address) {
			require.NoError(t, sh.Delete(aa))
		}),
		shard.WithGCRemoverSleepInterval(200 * time.Millisecond),
		shard.WithContainerPayments(&testContainerPayments{}),
	}

	sh = shard.New(opts...)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	t.Cleanup(func() {
		releaseShard(sh, t)
	})

	cnr := cidtest.ID()

	var expAttrObj, expAttrLocker object.Attribute
	expAttrObj.SetKey(object.AttributeExpirationEpoch)
	expAttrLocker.SetKey(object.AttributeExpirationEpoch)
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

	require.Eventually(t, func() bool {
		_, err = sh.Get(obj.Address(), false)
		return shard.IsErrObjectExpired(err)
	}, 3*time.Second, 1*time.Second, "lock expiration should make the object expired")

	epoch.Value = 6
	sh.NotificationChannel() <- shard.EventNewEpoch(epoch.Value)

	require.Eventually(t, func() bool {
		_, err = sh.Get(obj.Address(), false)
		return shard.IsErrNotFound(err)
	}, 3*time.Second, 1*time.Second, "expired object should eventually be deleted")
}

func TestGC_ContainerCleanup(t *testing.T) {
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

	require.Eventually(t, func() bool {
		containers, err = sh.ListContainers()
		require.NoError(t, err)

		for _, o := range oo {
			_, err = sh.Get(o, false)
			if !errors.Is(err, apistatus.ObjectNotFound{}) {
				return false
			}
		}

		return len(containers) == 0
	}, time.Second, 100*time.Millisecond)
}

func TestExpiration(t *testing.T) {
	rootPath := t.TempDir()
	var sh *shard.Shard

	opts := []shard.Option{
		shard.WithLogger(zaptest.NewLogger(t)),
		shard.WithBlobstor(fstree.New(
			fstree.WithPath(filepath.Join(rootPath, "fstree"))),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epochState{Value: 0}),
		),
		shard.WithExpiredObjectsCallback(
			func(addresses []oid.Address) {
				require.NoError(t, sh.Delete(addresses))
			},
		),
		shard.WithGCRemoverSleepInterval(100 * time.Millisecond),
		shard.WithContainerPayments(&testContainerPayments{}),
	}

	sh = shard.New(opts...)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	t.Cleanup(func() {
		releaseShard(sh, t)
	})
	ch := sh.NotificationChannel()

	var expAttr object.Attribute
	expAttr.SetKey(object.AttributeExpirationEpoch)

	obj := generateObject()

	for i, typ := range []object.Type{object.TypeRegular, object.TypeTombstone, object.TypeLink} {
		t.Run(fmt.Sprintf("type: %s", typ), func(t *testing.T) {
			exp := uint64(i * 10)

			expAttr.SetValue(strconv.FormatUint(exp, 10))
			obj.SetAttributes(expAttr)
			obj.SetType(typ)
			require.NoError(t, obj.SetIDWithSignature(neofscryptotest.Signer()))

			err := sh.Put(obj, nil)
			require.NoError(t, err)

			_, err = sh.Get(obj.Address(), false)
			require.NoError(t, err)

			ch <- shard.EventNewEpoch(exp + 1)

			require.Eventually(t, func() bool {
				_, err = sh.Get(obj.Address(), false)
				return shard.IsErrNotFound(err)
			}, 3*time.Second, 100*time.Millisecond, "expiration should lead to object removal")
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
		),
		shard.WithExpiredObjectsCallback(
			func(addresses []oid.Address) {
				require.NoError(t, sh.Delete(addresses))
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

	const unpaidWarningMsg = "found unpaid container"

	t.Run("paid container", func(t *testing.T) {
		err = sh.Put(obj, nil)
		require.NoError(t, err)

		ch <- shard.EventNewEpoch(1)
		ch <- shard.EventNewEpoch(1)
		ch <- shard.EventNewEpoch(1)
		ch <- shard.EventNewEpoch(1) // wait for epoch handling to finish at least once, its buffer is 1

		lb.AssertNotContainsMsg(zap.DebugLevel, unpaidWarningMsg)
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
			Level:   zap.WarnLevel,
			Message: unpaidWarningMsg,
			Fields: map[string]any{
				"epoch":       json.Number(strconv.FormatInt(currEpoch, 10)),
				"cID":         cID.String(),
				"unpaidSince": json.Number(strconv.FormatInt(unpaidSince, 10)),
			},
		}
		lb.AssertContains(expLog)
	})

	//for i, typ := range []object.Type{object.TypeRegular, object.TypeTombstone, object.TypeLink} {
	//	t.Run(fmt.Sprintf("type: %s", typ), func(t *testing.T) {
	//		exp := uint64(i * 10)
	//
	//		expAttr.SetValue(strconv.FormatUint(exp, 10))
	//		obj.SetAttributes(expAttr)
	//		obj.SetType(typ)
	//		require.NoError(t, obj.SetIDWithSignature(neofscryptotest.Signer()))
	//
	//		err := sh.Put(obj, nil)
	//		require.NoError(t, err)
	//
	//		_, err = sh.Get(obj.Address(), false)
	//		require.NoError(t, err)
	//
	//		ch <- shard.EventNewEpoch(exp + 1)
	//
	//		require.Eventually(t, func() bool {
	//			_, err = sh.Get(obj.Address(), false)
	//			return shard.IsErrNotFound(err)
	//		}, 3*time.Second, 100*time.Millisecond, "expiration should lead to object removal")
	//	})
	//}
}
