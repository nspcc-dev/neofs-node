package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/stretchr/testify/require"
)

// from neofs-api basic ACL specification
const (
	privateContainer          uint32 = 0x1C8C8CCC
	publicContainerWithSticky uint32 = 0x3FFFFFFF
	readonlyContainer         uint32 = 0x1FFFCCFF
)

var (
	allOperations = []eacl.Operation{
		eacl.OperationGet, eacl.OperationPut, eacl.OperationDelete,
		eacl.OperationHead, eacl.OperationSearch, eacl.OperationRange,
		eacl.OperationRangeHash,
	}
)

func TestDefaultBasicACLs(t *testing.T) {
	t.Run("private", func(t *testing.T) {
		r := basicACLHelper(privateContainer)

		require.False(t, r.Sticky())

		for _, op := range allOperations {
			require.True(t, r.UserAllowed(op))
			require.False(t, r.OthersAllowed(op))
			if op == eacl.OperationDelete || op == eacl.OperationRange {
				require.False(t, r.SystemAllowed(op))
			} else {
				require.True(t, r.SystemAllowed(op))
			}
		}
	})

	t.Run("public with sticky", func(t *testing.T) {
		r := basicACLHelper(publicContainerWithSticky)

		require.True(t, r.Sticky())

		for _, op := range allOperations {
			require.True(t, r.UserAllowed(op))
			require.True(t, r.OthersAllowed(op))
			require.True(t, r.SystemAllowed(op))
		}
	})

	t.Run("read only", func(t *testing.T) {
		r := basicACLHelper(readonlyContainer)

		require.False(t, r.Sticky())

		for _, op := range allOperations {
			require.True(t, r.UserAllowed(op))
			require.True(t, r.SystemAllowed(op))

			if op == eacl.OperationDelete || op == eacl.OperationPut {
				require.False(t, r.OthersAllowed(op))
			} else {
				require.True(t, r.OthersAllowed(op))
			}
		}
	})
}
