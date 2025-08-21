package engine

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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

	ts := createTombstone(cnr, obj.GetID().EncodeToString())
	require.NoError(t, e.Put(ts, nil))
	require.NoError(t, e.Inhume(object.AddressOf(ts), 0, addr))

	_, err := e.Get(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)

	_, err = e.ReviveObject(addr)
	require.NoError(t, err)

	got, err := e.Get(addr)
	require.NoError(t, err)
	require.Equal(t, obj, got)

	// tombstone metadata should be dropped, so getting TS must fail with not found
	_, err = e.Get(object.AddressOf(ts))
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
}

func createTombstone(cnr cid.ID, objID string) *objectSDK.Object {
	ts := generateObjectWithCID(cnr)
	ts.SetType(objectSDK.TypeTombstone)
	addAttribute(ts, objectSDK.AttributeAssociatedObject, objID)
	addAttribute(ts, objectSDK.AttributeExpirationEpoch, "0")
	return ts
}
