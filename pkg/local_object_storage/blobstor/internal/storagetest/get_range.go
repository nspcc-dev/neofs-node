package storagetest

import (
	"math"
	"testing"
	"testing/iotest"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestGetRangeStream(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	t.Run("missing object", func(t *testing.T) {
		_, err := s.GetRangeStream(oidtest.Address(), 0, 1)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	testGetRangeStream := func(t *testing.T, objects []objectDesc) {
		payload := objects[0].obj.Payload()

		var start, stop uint64 = 11, 100
		if uint64(len(payload)) < stop {
			panic("unexpected: invalid test object generated")
		}

		t.Run("regular", func(t *testing.T) {
			stream, err := s.GetRangeStream(objects[0].addr, start, stop-start)
			require.NoError(t, err)
			require.NoError(t, iotest.TestReader(stream, payload[start:stop]))
		})

		t.Run("offset > len(payload)", func(t *testing.T) {
			_, err := s.GetRangeStream(objects[0].addr, uint64(len(payload)+10), 10)
			require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
		})

		t.Run("offset + length > len(payload)", func(t *testing.T) {
			_, err := s.GetRangeStream(objects[0].addr, 10, uint64(len(payload)))
			require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
		})

		t.Run("length is negative when converted to int64", func(t *testing.T) {
			_, err := s.GetRangeStream(objects[0].addr, 0, 1<<63)
			require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
		})

		t.Run("offset + length overflow uint64", func(t *testing.T) {
			_, err := s.GetRangeStream(objects[0].addr, 10, math.MaxUint64-2)
			require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
		})

		t.Run("zero range", func(t *testing.T) {
			for i := range objects {
				stream, err := s.GetRangeStream(objects[i].addr, 0, 0)
				require.NoError(t, err)
				pld := objects[i].obj.Payload()
				require.NoError(t, iotest.TestReader(stream, pld))
				require.NoError(t, stream.Close())
			}
		})
	}

	testGetRangeStream(t, prepare(t, 1, s, minSize, maxSize))
	testGetRangeStream(t, prepareBatch(t, 1, s, minSize, maxSize))
}
