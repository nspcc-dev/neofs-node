package engine

import (
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestDeleteBigObject(t *testing.T) {
	defer os.RemoveAll(t.Name())

	cnr := cidtest.ID()
	parentID := oidtest.ID()
	splitID := objectSDK.NewSplitID()

	parent := generateObjectWithCID(cnr)
	parent.SetID(parentID)
	parent.SetPayload(nil)

	const childCount = 10
	children := make([]*objectSDK.Object, childCount)
	childIDs := make([]oid.ID, childCount)
	for i := range children {
		children[i] = generateObjectWithCID(cnr)
		if i != 0 {
			children[i].SetPreviousID(childIDs[i-1])
		}
		if i == len(children)-1 {
			children[i].SetParent(parent)
		}
		children[i].SetSplitID(splitID)
		children[i].SetPayload([]byte{byte(i), byte(i + 1), byte(i + 2)})
		children[i].SetPayloadSize(3)
		childIDs[i] = children[i].GetID()
	}

	link := generateObjectWithCID(cnr)
	link.SetParent(parent)
	link.SetParentID(parentID)
	link.SetSplitID(splitID)
	link.SetChildren(childIDs...)

	s1 := testNewShard(t, 1)
	s2 := testNewShard(t, 2)
	s3 := testNewShard(t, 3)

	e := testNewEngineWithShards(s1, s2, s3)
	e.log = zaptest.NewLogger(t)
	defer e.Close()

	for i := range children {
		require.NoError(t, e.Put(children[i], nil))
	}
	require.NoError(t, e.Put(link, nil))

	var splitErr *objectSDK.SplitInfoError

	addrParent := object.AddressOf(parent)
	checkGetError(t, e, addrParent, &splitErr)

	addrLink := object.AddressOf(link)
	checkGetError(t, e, addrLink, nil)

	for i := range children {
		checkGetError(t, e, object.AddressOf(children[i]), nil)
	}

	err := e.Delete(addrParent)
	require.NoError(t, err)

	checkGetError(t, e, addrParent, &apistatus.ObjectNotFound{})
	checkGetError(t, e, addrLink, &apistatus.ObjectNotFound{})
	for i := range children {
		checkGetError(t, e, object.AddressOf(children[i]), &apistatus.ObjectNotFound{})
	}
}

func checkGetError(t *testing.T, e *StorageEngine, addr oid.Address, expected any) {
	_, err := e.Get(addr)
	if expected != nil {
		require.ErrorAs(t, err, expected)
	} else {
		require.NoError(t, err)
	}
}
