package storagegroup

import (
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

type mockedObjects struct {
	hdr *object.Object
}

func (x *mockedObjects) Get(address oid.Address) (object.Object, error) {
	return *x.hdr, nil
}

func (x *mockedObjects) Head(_ oid.Address) (any, error) {
	return x.hdr, nil
}

func TestCollectMembers(t *testing.T) {
	t.Run("missing member's child homomorphic checksum", func(t *testing.T) {
		var child object.Object
		child.SetID(oidtest.ID())

		src := &mockedObjects{hdr: &child}

		_, err := CollectMembers(src, cidtest.ID(), []oid.ID{oidtest.ID()}, true)
		require.ErrorIs(t, err, errMissingHomomorphicChecksum)
	})

	t.Run("invalid member's child homomorphic checksum", func(t *testing.T) {
		var child object.Object
		child.SetID(oidtest.ID())

		var cs checksum.Checksum
		cs.SetSHA256([sha256.Size]byte{1}) // any non-homomorphic

		child.SetPayloadHomomorphicHash(cs)

		src := &mockedObjects{hdr: &child}

		_, err := CollectMembers(src, cidtest.ID(), []oid.ID{oidtest.ID()}, true)
		require.ErrorIs(t, err, errInvalidHomomorphicChecksum)
	})

	t.Run("missing member's child ID", func(t *testing.T) {
		var child object.Object

		_, ok := child.ID()
		require.False(t, ok)

		src := &mockedObjects{hdr: &child}

		_, err := CollectMembers(src, cidtest.ID(), []oid.ID{oidtest.ID()}, false)
		require.ErrorIs(t, err, errMissingSplitMemberID)
	})
}
