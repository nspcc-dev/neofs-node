package shard

import (
	"errors"
	"fmt"
	"io"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestShard_GetECPart(t *testing.T) {
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	pi := iec.PartInfo{
		RuleIndex: 123,
		Index:     456,
	}

	var parentObj object.Object
	parentObj.SetContainerID(cnr)
	parentObj.SetID(parentID)

	partObj, err := iec.FormObjectForECPart(neofscryptotest.Signer(), parentObj, testutil.RandByteSlice(32), pi)
	require.NoError(t, err)

	partID := partObj.GetID()
	partAddr := oid.NewAddress(cnr, partID)

	mb := mockMetabase{
		resolveECPart: map[resolveECPartKey]resolveECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {id: partID},
		},
	}
	bs := mockBLOBStore{
		getStream: map[oid.Address]getStreamValue{
			partAddr: {obj: partObj},
		},
	}

	// metabase errors
	for _, tc := range []struct {
		name      string
		err       error
		assertErr func(t *testing.T, err error)
	}{
		{name: "internal error", err: errors.New("internal error"), assertErr: func(t *testing.T, err error) {
			require.ErrorContains(t, err, "internal error")
		}},
		{name: "object not found", err: apistatus.ErrObjectNotFound, assertErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		}},
		{name: "object already removed", err: apistatus.ErrObjectAlreadyRemoved, assertErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "object expired", err: meta.ErrObjectIsExpired, assertErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, meta.ErrObjectIsExpired)
		}},
	} {
		t.Run("metabase/"+tc.name, func(t *testing.T) {
			mdb := mockMetabase{
				resolveECPart: map[resolveECPartKey]resolveECPartValue{
					{cnr: cnr, parent: parentID, pi: pi}: {err: tc.err},
				},
			}

			s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mdb, unimplementedWriteCache{})

			_, _, err := s.GetECPart(cnr, parentID, pi)
			require.ErrorContains(t, err, "resolve part ID in metabase")
			tc.assertErr(t, err)
		})
	}

	// BLOB storage errors
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "internal error", err: errors.New("internal error")},
		{name: "object not found", err: apistatus.ErrObjectNotFound},
	} {
		t.Run("BLOB storage/"+tc.name, func(t *testing.T) {
			bs := mockBLOBStore{
				getStream: map[oid.Address]getStreamValue{
					partAddr: {err: tc.err},
				},
			}

			s := newSimpleTestShard(t, &bs, &mb, nil)

			_, _, err := s.GetECPart(cnr, parentID, pi)
			require.ErrorIs(t, err, tc.err)
			require.ErrorContains(t, err, fmt.Sprintf("get from BLOB storage by ID %s", partID))
		})
	}

	t.Run("writecache", func(t *testing.T) {
		// errors
		for _, tc := range []struct {
			name   string
			err    error
			logMsg testutil.LogEntry
		}{
			{name: "internal error", err: errors.New("internal error"), logMsg: testutil.LogEntry{Fields: map[string]any{
				"partAddr": partAddr.String(),
				"error":    "internal error",
			}, Level: zap.InfoLevel, Message: "failed to get EC part object from write-cache, fallback to BLOB storage"}},
			{name: "object not found", err: fmt.Errorf("wrapped: %w", apistatus.ErrObjectNotFound), logMsg: testutil.LogEntry{Fields: map[string]any{
				"partAddr": partAddr.String(),
				"error":    "wrapped: " + apistatus.ErrObjectNotFound.Error(),
			}, Level: zap.DebugLevel, Message: "EC part object is missing in write-cache, fallback to BLOB storage"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

				wc := mockWriteCache{
					getStream: map[oid.Address]getStreamValue{
						oid.NewAddress(cnr, partID): {err: tc.err},
					},
				}

				s := newSimpleTestShard(t, &bs, &mb, &wc)
				s.log = l

				hdr, rdr, err := s.GetECPart(cnr, parentID, pi)
				require.NoError(t, err)
				assertGetECPartOK(t, partObj, hdr, rdr)

				lb.AssertSingle(tc.logMsg)
			})
		}

		wc := mockWriteCache{
			getStream: map[oid.Address]getStreamValue{
				oid.NewAddress(cnr, partID): {obj: partObj},
			},
		}

		s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mb, &wc)

		hdr, rdr, err := s.GetECPart(cnr, parentID, pi)
		require.NoError(t, err)
		assertGetECPartOK(t, partObj, hdr, rdr)
	})

	s := newSimpleTestShard(t, &bs, &mb, nil)

	hdr, rdr, err := s.GetECPart(cnr, parentID, pi)
	require.NoError(t, err)
	assertGetECPartOK(t, partObj, hdr, rdr)
}

func assertGetECPartOK(t testing.TB, exp, hdr object.Object, rdr io.ReadCloser) {
	b, err := io.ReadAll(rdr)
	require.NoError(t, err)
	hdr.SetPayload(b)
	require.Equal(t, exp, hdr)
}
