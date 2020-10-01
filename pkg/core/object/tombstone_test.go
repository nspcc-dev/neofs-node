package object

import (
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestTombstoneContent_MarshalBinary(t *testing.T) {
	cid1 := container.NewID()
	cid1.SetSHA256([sha256.Size]byte{1, 2})

	id1 := object.NewID()
	id1.SetSHA256([sha256.Size]byte{3, 4})

	addr1 := object.NewAddress()
	addr1.SetObjectID(id1)
	addr1.SetContainerID(cid1)

	cid2 := container.NewID()
	cid2.SetSHA256([sha256.Size]byte{5, 6})

	id2 := object.NewID()
	id2.SetSHA256([sha256.Size]byte{7, 8})

	addr2 := object.NewAddress()
	addr2.SetObjectID(id2)
	addr2.SetContainerID(cid2)

	c := NewTombstoneContent()
	c.SetAddressList(addr1, addr2)

	data, err := c.MarshalBinary()
	require.NoError(t, err)

	c2, err := TombstoneContentFromBytes(data)
	require.NoError(t, err)

	require.Equal(t, c, c2)
}
