package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	statusSDK "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestChildrenExpiration(t *testing.T) {
	const numOfShards = 5
	const currEpoch = 10
	es := &epochState{e: currEpoch}
	owner := usertest.ID()

	e := New()
	for i := range numOfShards {
		_, err := e.AddShard(
			shard.WithBlobStorOptions(
				blobstor.WithStorages(
					newStorages(filepath.Join(t.TempDir(), fmt.Sprintf("blobstor%d", i)),
						1<<20)),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(t.TempDir(), fmt.Sprintf("metabase%d", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(es),
			),
			shard.WithPiloramaOptions(
				pilorama.WithPath(filepath.Join(t.TempDir(), fmt.Sprintf("pilorama%d", i)))),
			shard.WithExpiredObjectsCallback(e.processExpiredObjects),
			shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
				pool, err := ants.NewPool(sz)
				if err != nil {
					panic(err)
				}

				return pool
			}),
		)
		require.NoError(t, err)
	}
	require.NoError(t, e.Open())
	require.NoError(t, e.Init())
	t.Cleanup(func() {
		_ = e.Close()
	})

	expAttr := *objectSDK.NewAttribute(objectSDK.AttributeExpirationEpoch, fmt.Sprint(currEpoch))

	t.Run("V1", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := objectSDK.NewSplitID()

		parent := generateObjectWithCID(cnr)
		parentID, _ := parent.ID()
		parent.SetAttributes(expAttr)

		child1 := generateObjectWithCID(cnr)
		child1ID, _ := child1.ID()
		child1.SetSplitID(splitID)

		child2 := generateObjectWithCID(cnr)
		child2ID, _ := child2.ID()
		child2.SetSplitID(splitID)
		child2.SetPreviousID(child1ID)

		child3 := generateObjectWithCID(cnr)
		child3ID, _ := child3.ID()
		child3.SetSplitID(splitID)
		child3.SetPreviousID(child2ID)
		child3.SetParent(parent)
		child3.SetParentID(parentID)

		link := generateObjectWithCID(cnr)
		link.SetParent(parent)
		link.SetParentID(parentID)
		link.SetChildren(child1ID, child2ID, child3ID)
		link.SetSplitID(splitID)

		require.NoError(t, Put(e, child1))
		require.NoError(t, Put(e, child2))
		require.NoError(t, Put(e, child3))
		require.NoError(t, Put(e, link))

		e.HandleNewEpoch(currEpoch + 1)

		checkObjectsAsyncRemoval(t, e, cnr, child1ID, child2ID, child3ID)
	})

	t.Run("V2", func(t *testing.T) {
		cnr := cidtest.ID()

		parent := generateObjectWithCID(cnr)
		parentID, _ := parent.ID()
		parent.SetAttributes(expAttr)

		child1 := generateObjectWithCID(cnr)
		child1ID, _ := child1.ID()
		child1.SetParent(parent)

		child2 := generateObjectWithCID(cnr)
		child2ID, _ := child2.ID()
		child2.SetFirstID(child1ID)
		child2.SetPreviousID(child1ID)

		child3 := generateObjectWithCID(cnr)
		child3ID, _ := child3.ID()
		child3.SetFirstID(child1ID)
		child3.SetPreviousID(child2ID)
		child3.SetParent(parent)
		child3.SetParentID(parentID)

		children := make([]objectSDK.MeasuredObject, 3)
		children[0].SetObjectID(child1ID)
		children[1].SetObjectID(child2ID)
		children[2].SetObjectID(child3ID)

		var link objectSDK.Link
		link.SetObjects(children)

		var linkObj objectSDK.Object
		linkObj.WriteLink(link)
		linkObj.SetContainerID(cnr)
		linkObj.SetParent(parent)
		linkObj.SetParentID(parentID)
		linkObj.SetFirstID(child1ID)
		linkObj.SetOwnerID(&owner)
		linkObj.CalculateAndSetPayloadChecksum()
		require.NoError(t, linkObj.CalculateAndSetID())

		require.NoError(t, Put(e, child1))
		require.NoError(t, Put(e, child2))
		require.NoError(t, Put(e, child3))
		require.NoError(t, Put(e, &linkObj))

		e.HandleNewEpoch(currEpoch + 1)

		checkObjectsAsyncRemoval(t, e, cnr, child1ID, child2ID, child3ID)
	})
}

func checkObjectsAsyncRemoval(t *testing.T, e *StorageEngine, cnr cid.ID, objs ...oid.ID) {
	require.Eventually(t, func() bool {
		var addr oid.Address
		addr.SetContainer(cnr)

		for _, obj := range objs {
			addr.SetObject(obj)

			_, err := Get(e, addr)
			if !errors.As(err, new(statusSDK.ObjectNotFound)) {
				return false
			}
		}

		return true
	}, 1*time.Second, 100*time.Millisecond)
}
