package engine

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_ReviveObject(t *testing.T) {
	e := testNewEngineWithShardNum(t, 1)
	defer func() { _ = e.Close() }()

	cnr := cidtest.ID()

	obj := generateObjectWithCID(cnr)
	require.NoError(t, e.Put(obj, nil))
	addr := object.AddressOf(obj)

	ts := generateObjectWithCID(cnr)
	ts.SetType(objectSDK.TypeTombstone)
	addAttribute(ts, objectSDK.AttributeAssociatedObject, obj.GetID().EncodeToString())
	addAttribute(ts, objectSDK.AttributeExpirationEpoch, "0")
	tsAddr := object.AddressOf(ts)

	require.NoError(t, e.Put(ts, nil))

	_, err := e.Get(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)

	rs, err := e.ReviveObject(addr)
	require.NoError(t, err)
	require.Equal(t, meta.ReviveStatusGraveyard, rs.Shards[0].Status.StatusType())
	require.Equal(t, tsAddr, rs.Shards[0].Status.TombstoneAddress())

	got, err := e.Get(addr)
	require.NoError(t, err)
	require.Equal(t, obj, got)

	// tombstone metadata should be dropped, so getting TS must fail with not found
	_, err = e.Get(tsAddr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
}
