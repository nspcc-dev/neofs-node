package shard_test

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestGC_ExpiredObjectWithExpiredLock(t *testing.T) {
	var sh *shard.Shard

	epoch := &epochState{
		Value: 10,
	}

	rootPath := t.TempDir()
	opts := []shard.Option{
		shard.WithLogger(zap.NewNop()),
		shard.WithBlobStorOptions(
			blobstor.WithStorages([]blobstor.SubStorage{
				{
					Storage: peapod.New(
						filepath.Join(rootPath, "blob", "peapod"),
						0o600,
						time.Second,
					),
					Policy: func(_ *object.Object, data []byte) bool {
						return len(data) <= 1<<20
					},
				},
				{
					Storage: fstree.New(
						fstree.WithPath(filepath.Join(rootPath, "blob"))),
				},
			}),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epoch),
		),
		shard.WithDeletedLockCallback(func(aa []oid.Address) {
			sh.HandleDeletedLocks(aa)
		}),
		shard.WithExpiredLocksCallback(func(aa []oid.Address) {
			sh.HandleExpiredLocks(aa)
		}),
		shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
			pool, err := ants.NewPool(sz)
			require.NoError(t, err)

			return pool
		}),
	}

	sh = shard.New(opts...)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	t.Cleanup(func() {
		releaseShard(sh, t)
	})

	cnr := cidtest.ID()

	var expAttr object.Attribute
	expAttr.SetKey(object.AttributeExpirationEpoch)
	expAttr.SetValue("1")

	obj := generateObjectWithCID(cnr)
	obj.SetAttributes(expAttr)
	objID := obj.GetID()

	expAttr.SetValue("3")

	lock := generateObjectWithCID(cnr)
	lock.SetType(object.TypeLock)
	lock.SetAttributes(expAttr)
	lockID := lock.GetID()

	err := sh.Put(obj, nil, 0)
	require.NoError(t, err)

	err = sh.Lock(cnr, lockID, []oid.ID{objID})
	require.NoError(t, err)

	err = sh.Put(lock, nil, 0)
	require.NoError(t, err)

	epoch.Value = 5
	sh.NotificationChannel() <- shard.EventNewEpoch(epoch.Value)

	require.Eventually(t, func() bool {
		_, err = sh.Get(objectCore.AddressOf(obj), false)
		return shard.IsErrNotFound(err)
	}, 3*time.Second, 1*time.Second, "lock expiration should free object removal")
}

func TestGC_ContainerCleanup(t *testing.T) {
	sh := newCustomShard(t, t.TempDir(), true,
		nil,
		nil,
		shard.WithGCRemoverSleepInterval(10*time.Millisecond))
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

		err := sh.Put(obj, nil, 0)
		require.NoError(t, err)

		oo = append(oo, objectCore.AddressOf(obj))
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
		shard.WithLogger(zap.NewNop()),
		shard.WithBlobStorOptions(
			blobstor.WithStorages([]blobstor.SubStorage{
				{
					Storage: fstree.New(
						fstree.WithPath(filepath.Join(rootPath, "blob"))),
					Policy: func(_ *object.Object, _ []byte) bool { return true },
				},
			}),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epochState{Value: math.MaxUint64 / 2}),
		),
		shard.WithExpiredObjectsCallback(
			func(addresses []oid.Address) {
				err := sh.MarkGarbage(false, addresses...)
				require.NoError(t, err)
			},
		),
		shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
			pool, err := ants.NewPool(sz)
			require.NoError(t, err)

			return pool
		}),
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

	for i, typ := range []object.Type{object.TypeRegular, object.TypeTombstone, object.TypeLink, object.TypeStorageGroup} {
		t.Run(fmt.Sprintf("type: %s", typ), func(t *testing.T) {
			exp := uint64(i * 10)

			expAttr.SetValue(strconv.FormatUint(exp, 10))
			obj.SetAttributes(expAttr)
			obj.SetType(typ)
			require.NoError(t, obj.SetIDWithSignature(neofscryptotest.Signer()))

			err := sh.Put(obj, nil, 0)
			require.NoError(t, err)

			_, err = sh.Get(objectCore.AddressOf(obj), false)
			require.NoError(t, err)

			ch <- shard.EventNewEpoch(exp + 1)

			require.Eventually(t, func() bool {
				_, err = sh.Get(objectCore.AddressOf(obj), false)
				return shard.IsErrNotFound(err)
			}, 3*time.Second, 100*time.Millisecond, "lock expiration should free object removal")
		})
	}
}
