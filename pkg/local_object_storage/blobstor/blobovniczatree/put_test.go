package blobovniczatree_test

import (
	"testing"

	. "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestSingleDir(t *testing.T) {
	tree := NewBlobovniczaTree(
		WithRootPath(t.TempDir()),
		WithBlobovniczaShallowDepth(0),
		WithBlobovniczaShallowWidth(10),
	)

	require.NoError(t, tree.Open(false))
	defer func() { _ = tree.Close() }()
	require.NoError(t, tree.Init())

	obj := objecttest.Object(t)
	bObj, err := obj.Marshal()
	require.NoError(t, err)

	putPrm := common.PutPrm{
		Address: oidtest.Address(),
		RawData: bObj,
	}

	_, err = tree.Put(putPrm)
	require.NoError(t, err)

	_, err = tree.Get(common.GetPrm{
		Address: putPrm.Address,
	})
	require.NoError(t, err)
}
