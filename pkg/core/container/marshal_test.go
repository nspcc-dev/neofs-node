package container

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
	"github.com/stretchr/testify/require"
)

func TestContainerMarshal(t *testing.T) {
	srcCnr := new(Container)
	srcCnr.SetBasicACL(basic.FromUint32(1))
	srcCnr.SetOwnerID(OwnerID{1, 2, 3})
	srcCnr.SetSalt([]byte{4, 5, 6})
	srcCnr.SetPlacementRule(PlacementRule{
		ReplFactor: 3,
	})

	data, err := srcCnr.MarshalBinary()
	require.NoError(t, err)

	dstCnr := new(Container)
	require.NoError(t, dstCnr.UnmarshalBinary(data))

	require.Equal(t, srcCnr, dstCnr)
}
