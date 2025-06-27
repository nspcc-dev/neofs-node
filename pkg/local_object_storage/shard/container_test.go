package shard_test

import (
	"context"
	"testing"

	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestShard_DeleteContainer(t *testing.T) {
	sh := newShard(t, true)
	defer releaseShard(sh, t)

	cID := cidtest.ID()

	o1 := generateObjectWithCID(cID)
	err := sh.Put(o1, nil)
	require.NoError(t, err)

	o2 := generateObjectWithCID(cID)
	o2.SetType(objectSDK.TypeStorageGroup)
	err = sh.Put(o2, nil)
	require.NoError(t, err)

	o3 := generateObjectWithCID(cID)
	o3.SetType(objectSDK.TypeLock)
	err = sh.Put(o3, nil)
	require.NoError(t, err)

	err = sh.DeleteContainer(context.Background(), cID)
	require.NoError(t, err)

	res, err := sh.Select(cID, nil)
	require.NoError(t, err)

	require.Empty(t, res)
}
