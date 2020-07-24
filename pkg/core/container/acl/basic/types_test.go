package basic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestACLValues(t *testing.T) {
	t.Run("private", func(t *testing.T) {
		acl := FromUint32(0x1C8C8CCC)

		require.False(t, acl.Reserved(0))
		require.False(t, acl.Reserved(1))
		require.False(t, acl.Sticky())
		require.True(t, acl.Final())

		require.True(t, acl.UserAllowed(OpGetRangeHash))
		require.True(t, acl.SystemAllowed(OpGetRangeHash))
		require.False(t, acl.OthersAllowed(OpGetRangeHash))
		require.False(t, acl.BearerAllowed(OpGetRangeHash))

		require.True(t, acl.UserAllowed(OpGetRange))
		require.False(t, acl.SystemAllowed(OpGetRange))
		require.False(t, acl.OthersAllowed(OpGetRange))
		require.False(t, acl.BearerAllowed(OpGetRange))

		require.True(t, acl.UserAllowed(OpSearch))
		require.True(t, acl.SystemAllowed(OpSearch))
		require.False(t, acl.OthersAllowed(OpSearch))
		require.False(t, acl.BearerAllowed(OpSearch))

		require.True(t, acl.UserAllowed(OpDelete))
		require.False(t, acl.SystemAllowed(OpDelete))
		require.False(t, acl.OthersAllowed(OpDelete))
		require.False(t, acl.BearerAllowed(OpDelete))

		require.True(t, acl.UserAllowed(OpPut))
		require.True(t, acl.SystemAllowed(OpPut))
		require.False(t, acl.OthersAllowed(OpPut))
		require.False(t, acl.BearerAllowed(OpPut))

		require.True(t, acl.UserAllowed(OpHead))
		require.True(t, acl.SystemAllowed(OpHead))
		require.False(t, acl.OthersAllowed(OpHead))
		require.False(t, acl.BearerAllowed(OpHead))

		require.True(t, acl.UserAllowed(OpGet))
		require.True(t, acl.SystemAllowed(OpGet))
		require.False(t, acl.OthersAllowed(OpGet))
		require.False(t, acl.BearerAllowed(OpGet))
	})

	t.Run("public with X-bit", func(t *testing.T) {
		acl := FromUint32(0x3FFFFFFF)

		require.False(t, acl.Reserved(0))
		require.False(t, acl.Reserved(1))
		require.True(t, acl.Sticky())
		require.True(t, acl.Final())

		require.True(t, acl.UserAllowed(OpGetRangeHash))
		require.True(t, acl.SystemAllowed(OpGetRangeHash))
		require.True(t, acl.OthersAllowed(OpGetRangeHash))
		require.True(t, acl.BearerAllowed(OpGetRangeHash))

		require.True(t, acl.UserAllowed(OpGetRange))
		require.True(t, acl.SystemAllowed(OpGetRange))
		require.True(t, acl.OthersAllowed(OpGetRange))
		require.True(t, acl.BearerAllowed(OpGetRange))

		require.True(t, acl.UserAllowed(OpSearch))
		require.True(t, acl.SystemAllowed(OpSearch))
		require.True(t, acl.OthersAllowed(OpSearch))
		require.True(t, acl.BearerAllowed(OpSearch))

		require.True(t, acl.UserAllowed(OpDelete))
		require.True(t, acl.SystemAllowed(OpDelete))
		require.True(t, acl.OthersAllowed(OpDelete))
		require.True(t, acl.BearerAllowed(OpDelete))

		require.True(t, acl.UserAllowed(OpPut))
		require.True(t, acl.SystemAllowed(OpPut))
		require.True(t, acl.OthersAllowed(OpPut))
		require.True(t, acl.BearerAllowed(OpPut))

		require.True(t, acl.UserAllowed(OpHead))
		require.True(t, acl.SystemAllowed(OpHead))
		require.True(t, acl.OthersAllowed(OpHead))
		require.True(t, acl.BearerAllowed(OpHead))

		require.True(t, acl.UserAllowed(OpGet))
		require.True(t, acl.SystemAllowed(OpGet))
		require.True(t, acl.OthersAllowed(OpGet))
		require.True(t, acl.BearerAllowed(OpGet))
	})

	t.Run("read only", func(t *testing.T) {
		acl := FromUint32(0x1FFFCCFF)

		require.False(t, acl.Reserved(0))
		require.False(t, acl.Reserved(1))
		require.False(t, acl.Sticky())
		require.True(t, acl.Final())

		require.True(t, acl.UserAllowed(OpGetRangeHash))
		require.True(t, acl.SystemAllowed(OpGetRangeHash))
		require.True(t, acl.OthersAllowed(OpGetRangeHash))
		require.True(t, acl.BearerAllowed(OpGetRangeHash))

		require.True(t, acl.UserAllowed(OpGetRange))
		require.True(t, acl.SystemAllowed(OpGetRange))
		require.True(t, acl.OthersAllowed(OpGetRange))
		require.True(t, acl.BearerAllowed(OpGetRange))

		require.True(t, acl.UserAllowed(OpSearch))
		require.True(t, acl.SystemAllowed(OpSearch))
		require.True(t, acl.OthersAllowed(OpSearch))
		require.True(t, acl.BearerAllowed(OpSearch))

		require.True(t, acl.UserAllowed(OpDelete))
		require.True(t, acl.SystemAllowed(OpDelete))
		require.False(t, acl.OthersAllowed(OpDelete))
		require.False(t, acl.BearerAllowed(OpDelete))

		require.True(t, acl.UserAllowed(OpPut))
		require.True(t, acl.SystemAllowed(OpPut))
		require.False(t, acl.OthersAllowed(OpPut))
		require.False(t, acl.BearerAllowed(OpPut))

		require.True(t, acl.UserAllowed(OpHead))
		require.True(t, acl.SystemAllowed(OpHead))
		require.True(t, acl.OthersAllowed(OpHead))
		require.True(t, acl.BearerAllowed(OpHead))

		require.True(t, acl.UserAllowed(OpGet))
		require.True(t, acl.SystemAllowed(OpGet))
		require.True(t, acl.OthersAllowed(OpGet))
		require.True(t, acl.BearerAllowed(OpGet))
	})
}

func TestACLMethods(t *testing.T) {
	acl := new(ACL)

	for i := uint8(0); i < reservedBitNumber; i++ {
		acl.SetReserved(i)
		require.True(t, acl.Reserved(i))
		acl.ResetReserved(i)
		require.False(t, acl.Reserved(i))
	}

	acl.SetSticky()
	require.True(t, acl.Sticky())
	acl.ResetSticky()
	require.False(t, acl.Sticky())

	acl.SetFinal()
	require.True(t, acl.Final())
	acl.ResetFinal()
	require.False(t, acl.Final())

	for i := OpGetRangeHash; i <= OpGet; i++ {
		acl.AllowUser(i)
		require.True(t, acl.UserAllowed(i))
		acl.ForbidUser(i)
		require.False(t, acl.UserAllowed(i))

		acl.AllowOthers(i)
		require.True(t, acl.OthersAllowed(i))
		acl.ForbidOthers(i)
		require.False(t, acl.OthersAllowed(i))

		acl.AllowBearer(i)
		require.True(t, acl.BearerAllowed(i))
		acl.ForbidBearer(i)
		require.False(t, acl.BearerAllowed(i))

		acl.AllowSystem(i)
		require.True(t, acl.SystemAllowed(i))
		acl.ForbidSystem(i)

		if i == OpDelete || i == OpGetRange {
			require.False(t, acl.SystemAllowed(i))
		} else {
			require.True(t, acl.SystemAllowed(i))
		}
	}
}
