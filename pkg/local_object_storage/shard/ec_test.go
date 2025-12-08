package shard

import (
	"errors"
	"fmt"
	"io"
	"math"
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

	t.Run("LINK", func(t *testing.T) {
		mb := meta.New(
			meta.WithPath(filepath.Join(t.TempDir(), "meta")),
			meta.WithEpochState(epochState{}),
			meta.WithLogger(zaptest.NewLogger(t)),
		)
		require.NoError(t, mb.Open(false))
		t.Cleanup(func() { _ = mb.Close() })
		require.NoError(t, mb.Init())

		payload := testutil.RandByteSlice(32) // any

		linker := *newObject(t)
		linker.SetContainerID(cnr)
		linker.SetParentID(parentID)
		linker.SetType(object.TypeLink)
		linker.SetPayload(payload)
		require.NoError(t, mb.Put(&linker))

		bs := mockBLOBStore{
			getStream: map[oid.Address]getStreamValue{
				objectcore.AddressOf(&linker): {obj: linker},
			},
		}

		s := newSimpleTestShard(t, &bs, mb, nil)

		hdr, rdr, err := s.GetECPart(cnr, parentID, pi)
		require.NoError(t, err)
		assertGetECPartOK(t, linker, hdr, rdr)

		hdr, rdr, err = s.GetECPart(cnr, linker.GetID(), pi)
		require.NoError(t, err)
		assertGetECPartOK(t, linker, hdr, rdr)
	})

	s := newSimpleTestShard(t, &bs, &mb, nil)

	hdr, rdr, err := s.GetECPart(cnr, parentID, pi)
	require.NoError(t, err)
	assertGetECPartOK(t, partObj, hdr, rdr)
}

func TestShard_GetECPartRange(t *testing.T) {
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
	partLen := partObj.PayloadSize()

	mb := mockMetabase{
		resolveECPartWithLen: map[resolveECPartKey]resolveECPartWithLenValue{
			{cnr: cnr, parent: parentID, pi: pi}: {id: partID, ln: partLen},
		},
	}
	bs := mockBLOBStore{
		getStream: map[oid.Address]getStreamValue{
			partAddr: {obj: partObj},
		},
	}

	t.Run("metabase errors", func(t *testing.T) {
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
			t.Run(tc.name, func(t *testing.T) {
				mdb := mockMetabase{
					resolveECPartWithLen: map[resolveECPartKey]resolveECPartWithLenValue{
						{cnr: cnr, parent: parentID, pi: pi}: {err: tc.err},
					},
				}

				s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mdb, unimplementedWriteCache{})

				_, _, err := s.GetECPartRange(cnr, parentID, pi, 0, 1)
				require.ErrorContains(t, err, "resolve part ID and payload len in metabase")
				tc.assertErr(t, err)
			})
		}

		t.Run("ID is returned", func(t *testing.T) {
			l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)
			metaErr := fmt.Errorf("%w failure: some cause", ierrors.ObjectID(partID))

			mb := mockMetabase{
				resolveECPartWithLen: map[resolveECPartKey]resolveECPartWithLenValue{
					{cnr: cnr, parent: parentID, pi: pi}: {err: metaErr},
				},
			}

			s := newSimpleTestShard(t, &bs, &mb, nil)
			s.log = l

			off, ln := int64(partLen/3), int64(partLen/2)
			gotLen, rdr, err := s.GetECPartRange(cnr, parentID, pi, off, ln)
			require.NoError(t, err)
			assertGetECPartRangeOK(t, partObj, off, ln, gotLen, rdr)

			lb.AssertSingle(testutil.LogEntry{
				Level:   zap.WarnLevel,
				Message: "EC part ID returned from metabase with error",
				Fields:  map[string]any{"container": cnr.String(), "parent": parentID.String(), "partID": partID.String(), "error": metaErr.Error()},
			})
		})

		t.Run("range out of bounds", func(t *testing.T) {
			mb := mockMetabase{
				resolveECPartWithLen: make(map[resolveECPartKey]resolveECPartWithLenValue),
			}

			s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mb, unimplementedWriteCache{})

			for _, rng := range [][2]uint64{
				{0, partLen + 1},
				{partLen - 1, 2},
				{partLen, 0},
				{partLen, 1},
				{math.MaxInt64, math.MaxInt64},
			} {
				mb.resolveECPartWithLen[resolveECPartKey{cnr: cnr, parent: parentID, pi: pi}] = resolveECPartWithLenValue{
					id: partID, ln: 0,
				}

				_, _, err := s.GetECPartRange(cnr, parentID, pi, int64(rng[0]), int64(rng[1]))
				require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange, rng)
			}
		})
	})

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

			_, _, err := s.GetECPartRange(cnr, parentID, pi, 0, 1)
			require.ErrorIs(t, err, tc.err)
			require.ErrorContains(t, err, fmt.Sprintf("get range by ID %s", partID))

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
				"object": partAddr.String(),
				"error":  "internal error",
			}, Level: zap.InfoLevel, Message: "failed to get object from write-cache, fallback to BLOB storage"}},
			{name: "object not found", err: fmt.Errorf("wrapped: %w", apistatus.ErrObjectNotFound), logMsg: testutil.LogEntry{Fields: map[string]any{
				"object": partAddr.String(),
				"error":  "wrapped: " + apistatus.ErrObjectNotFound.Error(),
			}, Level: zap.DebugLevel, Message: "object is missing in write-cache, fallback to BLOB storage"}},
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

				off, ln := int64(partLen/3), int64(partLen/2)
				gotLen, rdr, err := s.GetECPartRange(cnr, parentID, pi, off, ln)
				require.NoError(t, err)
				assertGetECPartRangeOK(t, partObj, off, ln, gotLen, rdr)

				lb.AssertSingle(tc.logMsg)
			})
		}

		wc := mockWriteCache{
			getStream: map[oid.Address]getStreamValue{
				partAddr: {obj: partObj},
			},
		}

		s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mb, &wc)
		testGetECPartRangeStream(t, partObj, parentID, pi, s)
	})

	t.Run("empty payload", func(t *testing.T) {
		mb := mockMetabase{
			resolveECPartWithLen: map[resolveECPartKey]resolveECPartWithLenValue{
				{cnr: cnr, parent: parentID, pi: pi}: {id: partID, ln: 0},
			},
		}

		s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mb, unimplementedWriteCache{})

		t.Run("non-zero range", func(t *testing.T) {
			_, _, err := s.GetECPartRange(cnr, parentID, pi, 0, 1)
			require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
		})

		pldLen, rc, err := s.GetECPartRange(cnr, parentID, pi, 0, 0)
		require.NoError(t, err)
		require.Zero(t, pldLen)
		require.Zero(t, rc)
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
			sysObj.SetPayloadSize(0)
			sysObj.SetPayload([]byte{})
			require.NoError(t, mb.Put(&sysObj))

			bs := mockBLOBStore{
				getStream: map[oid.Address]getStreamValue{
					objectcore.AddressOf(&sysObj): {obj: partObj},
				},
			}

			s := newSimpleTestShard(t, &bs, mb, nil)

			gotLen, rdr, err := s.GetECPartRange(cnr, sysObj.GetID(), pi, 0, 0)
			require.NoError(t, err)
			assertGetECPartRangeOK(t, sysObj, 0, 0, gotLen, rdr)

			_, _, err = s.GetECPartRange(cnr, sysObj.GetID(), pi, 0, 1)
			require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
		})
	}

	s := newSimpleTestShard(t, &bs, &mb, nil)
	testGetECPartRangeStream(t, partObj, parentID, pi, s)
}

func TestShard_HeadECPart(t *testing.T) {
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

	partHdr := *partObj.CutPayload()

	partID := partObj.GetID()
	partAddr := oid.NewAddress(cnr, partID)

	mb := mockMetabase{
		resolveECPart: map[resolveECPartKey]resolveECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {id: partID},
		},
	}
	bs := mockBLOBStore{
		head: map[oid.Address]headValue{
			partAddr: {hdr: partHdr},
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

			_, err := s.HeadECPart(cnr, parentID, pi)
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
				head: map[oid.Address]headValue{
					partAddr: {err: tc.err},
				},
			}

			s := newSimpleTestShard(t, &bs, &mb, nil)

			_, err := s.HeadECPart(cnr, parentID, pi)
			require.ErrorIs(t, err, tc.err)
			require.ErrorContains(t, err, fmt.Sprintf("get header from BLOB storage by ID %s", partID))

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
			}, Level: zap.InfoLevel, Message: "failed to get EC part object header from write-cache, fallback to BLOB storage"}},
			{name: "object not found", err: fmt.Errorf("wrapped: %w", apistatus.ErrObjectNotFound), logMsg: testutil.LogEntry{Fields: map[string]any{
				"partAddr": partAddr.String(),
				"error":    "wrapped: " + apistatus.ErrObjectNotFound.Error(),
			}, Level: zap.DebugLevel, Message: "EC part object is missing in write-cache, fallback to BLOB storage"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

				wc := mockWriteCache{
					head: map[oid.Address]headValue{
						partAddr: {err: tc.err},
					},
				}

				s := newSimpleTestShard(t, &bs, &mb, &wc)
				s.log = l

				hdr, err := s.HeadECPart(cnr, parentID, pi)
				require.NoError(t, err)
				require.Equal(t, partHdr, hdr)

				lb.AssertSingle(tc.logMsg)
			})
		}

		wc := mockWriteCache{
			head: map[oid.Address]headValue{
				partAddr: {hdr: partHdr},
			},
		}

		s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mb, &wc)

		hdr, err := s.HeadECPart(cnr, parentID, pi)
		require.NoError(t, err)
		require.Equal(t, partHdr, hdr)
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
			require.NoError(t, mb.Put(&sysObj))

			bs := mockBLOBStore{
				head: map[oid.Address]headValue{
					objectcore.AddressOf(&sysObj): {hdr: sysObj},
				},
			}

			s := newSimpleTestShard(t, &bs, mb, nil)

			hdr, err := s.HeadECPart(cnr, sysObj.GetID(), pi)
			require.NoError(t, err)
			require.Equal(t, sysObj, hdr)
		})
	}

	s := newSimpleTestShard(t, &bs, &mb, nil)

	hdr, err := s.HeadECPart(cnr, parentID, pi)
	require.NoError(t, err)
	require.Equal(t, partHdr, hdr)
}

func testGetECPartRangeStream(t *testing.T, obj object.Object, parent oid.ID, pi iec.PartInfo, s *Shard) {
	full := int64(obj.PayloadSize())
	for _, rng := range [][2]int64{
		{0, 0},
		{0, 1},
		{0, full},
		{full / 3, full / 2},
		{full / 2, full / 2},
		{full - 1, 1},
	} {
		off, ln := rng[0], rng[1]
		t.Run(fmt.Sprintf("full=%d,off=%d,len=%d", full, off, ln), func(t *testing.T) {
			pldLen, rc, err := s.GetECPartRange(obj.GetContainerID(), parent, pi, off, ln)
			require.NoError(t, err)
			assertGetECPartRangeOK(t, obj, off, ln, pldLen, rc)
		})
	}
}

func assertGetECPartOK(t testing.TB, exp, hdr object.Object, rdr io.ReadCloser) {
	b, err := io.ReadAll(rdr)
	require.NoError(t, err)
	hdr.SetPayload(b)
	require.Equal(t, exp, hdr)
}

func assertGetECPartRangeOK(t testing.TB, exp object.Object, off, ln int64, pldLen uint64, rc io.ReadCloser) {
	require.EqualValues(t, exp.PayloadSize(), pldLen)

	if pldLen == 0 {
		require.Zero(t, rc)
		require.Zero(t, off)
		require.Zero(t, ln)
		return
	}

	require.NotNil(t, rc)
	b, err := io.ReadAll(rc)
	require.NoError(t, err)

	if off == 0 && ln == 0 {
		ln = int64(pldLen)
	}

	require.Len(t, b, int(ln))

	require.Equal(t, exp.Payload()[off:][:ln], b)
	require.NoError(t, rc.Close())
}
