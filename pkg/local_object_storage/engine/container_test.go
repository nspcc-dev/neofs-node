package engine

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

type cnrSource struct{}

func (c cnrSource) Get(cid.ID) (container.Container, error) {
	return container.Container{}, apistatus.ContainerNotFound{} // value not used, only err
}

func TestStorageEngine_ContainerCleanUp(t *testing.T) {
	path := t.TempDir()

	synctest.Test(t, func(t *testing.T) {
		e := New(WithContainersSource(cnrSource{}))
		t.Cleanup(func() {
			_ = e.Close()
		})

		for i := range 5 {
			_, err := e.AddShard(
				shard.WithBlobstor(newStorage(filepath.Join(path, strconv.Itoa(i)))),
				shard.WithMetaBaseOptions(
					meta.WithPath(filepath.Join(path, fmt.Sprintf("%d.metabase", i))),
					meta.WithPermissions(0700),
					meta.WithEpochState(epochState{}),
				),
			)
			require.NoError(t, err)
		}
		o1 := objecttest.Object()
		o2 := objecttest.Object()
		o2.SetPayload(make([]byte, errSmallSize+1))

		err := e.Put(&o1, nil)
		require.NoError(t, err)

		err = e.Put(&o2, nil)
		require.NoError(t, err)

		require.NoError(t, e.Init())

		time.Sleep(time.Second)
		_, err1 := e.Get(o1.Address())
		_, err2 := e.Get(o2.Address())

		require.ErrorIs(t, err1, new(apistatus.ObjectNotFound))
		require.ErrorIs(t, err2, new(apistatus.ObjectNotFound))
	})
}
