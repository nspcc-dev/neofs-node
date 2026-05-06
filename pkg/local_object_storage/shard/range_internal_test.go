package shard

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestShard_GetRangeStream(t *testing.T) {
	cnr := cidtest.ID()
	id := oidtest.ID()

	const payloadLen = 4 << 10
	payload := testutil.RandByteSlice(payloadLen)

	var obj object.Object
	obj.SetContainerID(cnr)
	obj.SetID(id)
	obj.SetPayloadSize(payloadLen)
	obj.SetPayload(payload)

	objAddr := obj.Address()

	bs := mockBLOBStore{
		getRangeStream: map[oid.Address]getRangeStreamValue{
			objAddr: {pld: obj.Payload()},
		},
	}

	t.Run("BLOB storage failures", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			err  error
		}{
			{name: "internal error", err: errors.New("internal error")},
			{name: "object not found", err: apistatus.ErrObjectNotFound},
		} {
			t.Run(tc.name, func(t *testing.T) {
				bs := mockBLOBStore{
					getRangeStream: map[oid.Address]getRangeStreamValue{
						objAddr: {err: tc.err},
					},
				}

				s := newSimpleTestShard(t, &bs, unimplementedMetabase{}, nil)

				_, _, err := s.GetRangeStream(cnr, id, 0, 1)
				require.ErrorIs(t, err, tc.err)
				require.ErrorContains(t, err, "get from BLOB storage")
			})
		}

		t.Run("range out of bounds", func(t *testing.T) {
			bs := mockBLOBStore{
				getRangeStream: map[oid.Address]getRangeStreamValue{
					objAddr: {pld: obj.Payload()},
				},
			}

			s := newSimpleTestShard(t, &bs, unimplementedMetabase{}, nil)

			for _, rng := range [][2]uint64{
				{0, payloadLen + 1},
				{payloadLen - 1, 2},
				{payloadLen, 0},
				{payloadLen, 1},
				{math.MaxInt64, math.MaxInt64},
			} {
				_, _, err := s.GetRangeStream(cnr, id, rng[0], rng[1])
				require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange, rng)
			}
		})
	})

	t.Run("writecache", func(t *testing.T) {
		t.Run("failures", func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				err    error
				logMsg testutil.LogEntry
			}{
				{name: "internal error", err: errors.New("internal error"), logMsg: testutil.LogEntry{Fields: map[string]any{
					"object": objAddr.String(),
					"error":  "internal error",
				}, Level: zap.InfoLevel, Message: "failed to get object from write-cache, fallback to BLOB storage"}},
				{name: "object not found", err: fmt.Errorf("wrapped: %w", apistatus.ErrObjectNotFound), logMsg: testutil.LogEntry{Fields: map[string]any{
					"object": objAddr.String(),
					"error":  "wrapped: " + apistatus.ErrObjectNotFound.Error(),
				}, Level: zap.DebugLevel, Message: "object is missing in write-cache, fallback to BLOB storage"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

					wc := mockWriteCache{
						getRangeStream: map[oid.Address]getRangeStreamValue{
							objAddr: {err: tc.err},
						},
					}

					s := newSimpleTestShard(t, &bs, &unimplementedMetabase{}, &wc)
					s.log = l

					off, ln := uint64(payloadLen/2), uint64(payloadLen/2)
					pldLen, rc, err := s.GetRangeStream(cnr, id, off, ln)
					require.NoError(t, err)
					assertGetRangeStreamOK(t, obj, off, ln, pldLen, rc)

					lb.AssertSingle(tc.logMsg)
				})
			}

			t.Run("range out of bounds", func(t *testing.T) {
				wc := mockWriteCache{
					getRangeStream: map[oid.Address]getRangeStreamValue{
						objAddr: {pld: obj.Payload()},
					},
				}

				s := newSimpleTestShard(t, unimplementedBLOBStore{}, unimplementedMetabase{}, &wc)

				for _, rng := range [][2]uint64{
					{0, payloadLen + 1},
					{payloadLen - 1, 2},
					{payloadLen, 0},
					{payloadLen, 1},
					{math.MaxInt64, math.MaxInt64},
				} {
					_, _, err := s.GetRangeStream(cnr, id, rng[0], rng[1])
					require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange, rng)
				}
			})
		})

		t.Run("empty payload", func(t *testing.T) {
			obj := obj
			obj.SetPayloadSize(0)
			obj.SetPayload(nil)

			wc := mockWriteCache{
				getRangeStream: map[oid.Address]getRangeStreamValue{
					objAddr: {pld: obj.Payload()},
				},
			}

			s := newSimpleTestShard(t, unimplementedBLOBStore{}, &unimplementedMetabase{}, &wc)

			t.Run("non-zero range", func(t *testing.T) {
				_, _, err := s.GetRangeStream(cnr, id, 0, 1)
				require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
			})

			pldLen, rc, err := s.GetRangeStream(cnr, id, 0, 0)
			require.NoError(t, err)
			require.Zero(t, pldLen)
			require.Zero(t, rc)
		})

		wc := mockWriteCache{
			getRangeStream: map[oid.Address]getRangeStreamValue{
				objAddr: {pld: obj.Payload()},
			},
		}

		s := newSimpleTestShard(t, unimplementedBLOBStore{}, &unimplementedMetabase{}, &wc)
		testGetRangeStream(t, obj, s)
	})

	t.Run("empty payload", func(t *testing.T) {
		obj := obj
		obj.SetPayloadSize(0)
		obj.SetPayload(nil)

		bs := mockBLOBStore{
			getRangeStream: map[oid.Address]getRangeStreamValue{
				objAddr: {pld: obj.Payload()},
			},
		}

		s := newSimpleTestShard(t, &bs, &unimplementedMetabase{}, nil)

		t.Run("non-zero range", func(t *testing.T) {
			_, _, err := s.GetRangeStream(cnr, id, 0, 1)
			require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
		})

		pldLen, rc, err := s.GetRangeStream(cnr, id, 0, 0)
		require.NoError(t, err)
		require.Zero(t, pldLen)
		require.Zero(t, rc)
	})

	s := newSimpleTestShard(t, &bs, &unimplementedMetabase{}, nil)
	testGetRangeStream(t, obj, s)
}

func testGetRangeStream(t *testing.T, obj object.Object, s *Shard) {
	full := obj.PayloadSize()
	for _, rng := range [][2]uint64{
		{0, 0},
		{0, 1},
		{0, full},
		{full / 3, full / 2},
		{full / 2, full / 2},
		{full - 1, 1},
	} {
		off, ln := rng[0], rng[1]
		t.Run(fmt.Sprintf("full=%d,off=%d,len=%d", full, off, ln), func(t *testing.T) {
			pldLen, rc, err := s.GetRangeStream(obj.GetContainerID(), obj.GetID(), off, ln)
			require.NoError(t, err)
			assertGetRangeStreamOK(t, obj, off, ln, pldLen, rc)
		})
	}
}

func assertGetRangeStreamOK(t testing.TB, obj object.Object, off, ln uint64, pldLen uint64, rc io.ReadCloser) {
	require.EqualValues(t, obj.PayloadSize(), pldLen)

	b, err := io.ReadAll(rc)
	require.NoError(t, err)

	if off == 0 && ln == 0 {
		ln = pldLen
	}

	require.True(t, bytes.Equal(obj.Payload()[off:][:ln], b))
	require.NoError(t, rc.Close())
}
