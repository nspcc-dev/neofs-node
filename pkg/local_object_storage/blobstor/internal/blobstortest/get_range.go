package blobstortest

import (
	"math"
	"testing"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestGetRange(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 1, s, minSize, maxSize)

	t.Run("missing object", func(t *testing.T) {
		_, err := s.GetRange(oidtest.Address(), 0, 1)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	payload := objects[0].obj.Payload()

	var start, stop uint64 = 11, 100
	if uint64(len(payload)) < stop {
		panic("unexpected: invalid test object generated")
	}

	t.Run("regular", func(t *testing.T) {
		res, err := s.GetRange(objects[0].addr, start, stop-start)
		require.NoError(t, err)
		require.Equal(t, payload[start:stop], res)
	})

	t.Run("offset > len(payload)", func(t *testing.T) {
		_, err := s.GetRange(objects[0].addr, uint64(len(payload)+10), 10)
		require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
	})

	t.Run("offset + length > len(payload)", func(t *testing.T) {
		_, err := s.GetRange(objects[0].addr, 10, uint64(len(payload)))
		require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
	})

	t.Run("length is negative when converted to int64", func(t *testing.T) {
		_, err := s.GetRange(objects[0].addr, 0, 1<<63)
		require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
	})

	t.Run("offset + length overflow uint64", func(t *testing.T) {
		_, err := s.GetRange(objects[0].addr, 10, math.MaxUint64-2)
		require.ErrorAs(t, err, new(apistatus.ObjectOutOfRange))
	})
}
