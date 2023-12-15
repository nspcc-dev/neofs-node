package shard_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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
		shard.WithDeletedLockCallback(func(_ context.Context, aa []oid.Address) {
			sh.HandleDeletedLocks(aa)
		}),
		shard.WithExpiredLocksCallback(func(_ context.Context, aa []oid.Address) {
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

	var expAttr objectSDK.Attribute
	expAttr.SetKey(objectV2.SysAttributeExpEpoch)
	expAttr.SetValue("1")

	obj := generateObjectWithCID(t, cnr)
	obj.SetAttributes(expAttr)
	objID, _ := obj.ID()

	expAttr.SetValue("3")

	lock := generateObjectWithCID(t, cnr)
	lock.SetType(object.TypeLock)
	lock.SetAttributes(expAttr)
	lockID, _ := lock.ID()

	var putPrm shard.PutPrm
	putPrm.SetObject(obj)

	_, err := sh.Put(putPrm)
	require.NoError(t, err)

	err = sh.Lock(cnr, lockID, []oid.ID{objID})
	require.NoError(t, err)

	putPrm.SetObject(lock)
	_, err = sh.Put(putPrm)
	require.NoError(t, err)

	epoch.Value = 5
	sh.NotificationChannel() <- shard.EventNewEpoch(epoch.Value)

	var getPrm shard.GetPrm
	getPrm.SetAddress(objectCore.AddressOf(obj))
	require.Eventually(t, func() bool {
		_, err = sh.Get(getPrm)
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

	for i := 0; i < numOfObjs; i++ {
		var putPrm shard.PutPrm

		obj := generateObjectWithCID(t, cID)
		addAttribute(obj, fmt.Sprintf("foo%d", i), fmt.Sprintf("bar%d", i))
		if i%2 == 0 {
			addPayload(obj, 1<<5) // small
		} else {
			addPayload(obj, 1<<20) // big
		}
		putPrm.SetObject(obj)

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		oo = append(oo, objectCore.AddressOf(obj))
	}

	res, err := sh.ListContainers(shard.ListContainersPrm{})
	require.NoError(t, err)
	require.Len(t, res.Containers(), 1)

	for _, o := range oo {
		var getPrm shard.GetPrm
		getPrm.SetAddress(o)

		_, err = sh.Get(getPrm)
		require.NoError(t, err)
	}

	require.NoError(t, sh.InhumeContainer(cID))

	require.Eventually(t, func() bool {
		res, err = sh.ListContainers(shard.ListContainersPrm{})
		require.NoError(t, err)

		for _, o := range oo {
			var getPrm shard.GetPrm
			getPrm.SetAddress(o)

			_, err = sh.Get(getPrm)
			if !errors.Is(err, apistatus.ObjectNotFound{}) {
				return false
			}
		}

		return len(res.Containers()) == 0
	}, time.Second, 100*time.Millisecond)
}
