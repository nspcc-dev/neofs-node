package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
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

	e := New(WithContainersSource(cnrSource{}))
	t.Cleanup(func() {
		_ = e.Close()
	})

	for i := range 5 {
		_, err := e.AddShard(
			shard.WithBlobStorOptions(
				blobstor.WithStorages(newStorage(filepath.Join(path, strconv.Itoa(i))))),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(path, fmt.Sprintf("%d.metabase", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(epochState{}),
			),
		)
		require.NoError(t, err)
	}
	require.NoError(t, e.Open())

	o1 := objecttest.Object()
	o2 := objecttest.Object()
	o2.SetPayload(make([]byte, errSmallSize+1))

	err := e.Put(&o1, nil, 0)
	require.NoError(t, err)

	err = e.Put(&o2, nil, 0)
	require.NoError(t, err)

	require.NoError(t, e.Init())

	require.Eventually(t, func() bool {
		_, err1 := e.Get(object.AddressOf(&o1))
		_, err2 := e.Get(object.AddressOf(&o2))

		return errors.Is(err1, new(apistatus.ObjectNotFound)) && errors.Is(err2, new(apistatus.ObjectNotFound))
	}, time.Second, 100*time.Millisecond)
}
