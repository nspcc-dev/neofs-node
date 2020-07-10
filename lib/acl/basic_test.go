package acl

import (
	"math/bits"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/stretchr/testify/require"
)

func TestBasicACLChecker(t *testing.T) {
	reqs := []object.RequestType{
		object.RequestGet,
		object.RequestHead,
		object.RequestPut,
		object.RequestDelete,
		object.RequestSearch,
		object.RequestRange,
		object.RequestRangeHash,
	}

	targets := []acl.Target{
		acl.Target_Others,
		acl.Target_System,
		acl.Target_User,
	}

	checker := new(BasicACLChecker)

	t.Run("verb permissions", func(t *testing.T) {
		mask := uint32(1)

		for i := range reqs {
			res, err := checker.Bearer(mask, reqs[i])
			require.NoError(t, err)
			require.True(t, res)

			mask = bits.Reverse32(mask)
			res, err = checker.Bearer(mask, reqs[i])
			require.NoError(t, err)
			require.False(t, res)

			mask = bits.Reverse32(mask)

			for j := range targets {
				mask <<= 1
				res, err = checker.Action(mask, reqs[i], targets[j])
				require.NoError(t, err)
				require.True(t, res)

				mask = bits.Reverse32(mask)
				res, err = checker.Action(mask, reqs[i], targets[j])
				require.NoError(t, err)
				require.False(t, res)

				mask = bits.Reverse32(mask)
			}
			mask <<= 1
		}
	})

	t.Run("unknown verb", func(t *testing.T) {
		mask := uint32(1)
		_, err := checker.Bearer(mask, -1)
		require.Error(t, err)

		mask = 2
		_, err = checker.Action(mask, -1, acl.Target_Others)
		require.Error(t, err)
	})

	t.Run("unknown action", func(t *testing.T) {
		mask := uint32(2)
		_, err := checker.Action(mask, object.RequestGet, -1)
		require.Error(t, err)
	})

	t.Run("extended acl permission", func(t *testing.T) {
		// set F-bit
		mask := uint32(0) | aclFinalBit
		require.False(t, checker.Extended(mask))

		// unset F-bit
		mask = bits.Reverse32(mask)
		require.True(t, checker.Extended(mask))
	})

	t.Run("sticky bit permission", func(t *testing.T) {
		mask := uint32(0x20000000)
		require.True(t, checker.Sticky(mask))

		mask = bits.Reverse32(mask)
		require.False(t, checker.Sticky(mask))
	})
}

// todo: add tests like in basic acl checker
func TestNeoFSMaskedBasicACLChecker(t *testing.T) {
	const orFilter = 0x04040444 // this OR filter will be used in neofs-node
	checker := NewMaskedBasicACLChecker(orFilter, DefaultAndFilter)

	reqs := []object.RequestType{
		object.RequestGet,
		object.RequestHead,
		object.RequestPut,
		object.RequestSearch,
		object.RequestRangeHash,
	}

	for i := range reqs {
		res, err := checker.Action(0, reqs[i], acl.Target_System)
		require.NoError(t, err)
		require.True(t, res)
	}
}
