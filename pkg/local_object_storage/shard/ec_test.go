package shard

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
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

			var oidErr ierrors.ObjectID
			require.ErrorAs(t, err, &oidErr)
			require.EqualValues(t, partID, oidErr)
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

	for _, tc := range []struct {
		typ       object.Type
		associate func(*object.Object, oid.ID)
	}{
		{typ: object.TypeTombstone, associate: (*object.Object).AssociateDeleted},
		{typ: object.TypeLock, associate: (*object.Object).AssociateLocked},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			mb := meta.New(
				meta.WithPath(filepath.Join(t.TempDir(), "meta")),
				meta.WithEpochState(epochState{}),
				meta.WithLogger(zaptest.NewLogger(t)),
			)
			require.NoError(t, mb.Open(false))
			t.Cleanup(func() { _ = mb.Close() })
			require.NoError(t, mb.Init())

			sysObj := *newObject(t)
			sysObj.SetContainerID(cnr)
			tc.associate(&sysObj, oidtest.ID())
			sysObj.SetPayload([]byte{})
			require.NoError(t, mb.Put(&sysObj))

			bs := mockBLOBStore{
				getStream: map[oid.Address]getStreamValue{
					objectcore.AddressOf(&sysObj): {obj: sysObj},
				},
			}

			s := newSimpleTestShard(t, &bs, mb, nil)

			hdr, rdr, err := s.GetECPart(cnr, sysObj.GetID(), pi)
			require.NoError(t, err)
			assertGetECPartOK(t, sysObj, hdr, rdr)
		})
	}

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
