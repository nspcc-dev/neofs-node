package object

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap/epoch"
	"github.com/stretchr/testify/require"
)

func TestSystemHeader_Version(t *testing.T) {
	h := new(SystemHeader)

	v := uint64(7)
	h.SetVersion(v)

	require.Equal(t, v, h.Version())
}

func TestSystemHeader_PayloadLength(t *testing.T) {
	h := new(SystemHeader)

	ln := uint64(3)
	h.SetPayloadLength(ln)

	require.Equal(t, ln, h.PayloadLength())
}

func TestSystemHeader_ID(t *testing.T) {
	h := new(SystemHeader)

	id := ID{1, 2, 3}
	h.SetID(id)

	require.Equal(t, id, h.ID())
}

func TestSystemHeader_CID(t *testing.T) {
	h := new(SystemHeader)

	cid := CID{1, 2, 3}
	h.SetCID(cid)

	require.Equal(t, cid, h.CID())
}

func TestSystemHeader_OwnerID(t *testing.T) {
	h := new(SystemHeader)

	ownerID := OwnerID{1, 2, 3}
	h.SetOwnerID(ownerID)

	require.Equal(t, ownerID, h.OwnerID())
}

func TestSystemHeader_CreationEpoch(t *testing.T) {
	h := new(SystemHeader)

	ep := epoch.FromUint64(1)
	h.SetCreationEpoch(ep)

	require.True(t, epoch.EQ(ep, h.CreationEpoch()))
}
