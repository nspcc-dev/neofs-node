package blobstortest

import (
	"math"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestGetRange(t *testing.T, cons Constructor, min, max uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 1, s, min, max)

	t.Run("missing object", func(t *testing.T) {
		gPrm := common.GetRangePrm{Address: oidtest.Address()}
		_, err := s.GetRange(gPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	payload := objects[0].obj.Payload()

	var start, stop uint64 = 11, 100
	if uint64(len(payload)) < stop {
		panic("unexpected: invalid test object generated")
	}

	var gPrm common.GetRangePrm
	gPrm.Address = objects[0].addr
	gPrm.Range.SetOffset(start)
	gPrm.Range.SetLength(stop - start)

	t.Run("without storage ID", func(t *testing.T) {
		// Without storage ID.
		res, err := s.GetRange(gPrm)
		require.NoError(t, err)
		require.Equal(t, payload[start:stop], res.Data)
	})

	t.Run("with storage ID", func(t *testing.T) {
		gPrm.StorageID = objects[0].storageID
		res, err := s.GetRange(gPrm)
		require.NoError(t, err)
		require.Equal(t, payload[start:stop], res.Data)
	})

	t.Run("offset > len(payload)", func(t *testing.T) {
		gPrm.Range.SetOffset(uint64(len(payload) + 10))
		gPrm.Range.SetLength(10)

		_, err := s.GetRange(gPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
	})

	t.Run("offset + length > len(payload)", func(t *testing.T) {
		gPrm.Range.SetOffset(10)
		gPrm.Range.SetLength(uint64(len(payload)))

		_, err := s.GetRange(gPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
	})

	t.Run("length is negative when converted to int64", func(t *testing.T) {
		gPrm.Range.SetOffset(0)
		gPrm.Range.SetLength(1 << 63)

		_, err := s.GetRange(gPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
	})

	t.Run("offset + length overflow uint64", func(t *testing.T) {
		gPrm.Range.SetOffset(10)
		gPrm.Range.SetLength(math.MaxUint64 - 2)

		_, err := s.GetRange(gPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
	})
}
