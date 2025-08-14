package shard

import (
	"encoding/json"
	"errors"
	"fmt"
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
	"go.uber.org/zap/zapcore"
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
		get: map[oid.Address]getValue{
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
			require.EqualError(t, err, "resolve part ID in metabase: internal error")
		}},
		{name: "degraded", err: meta.ErrDegradedMode, assertErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, ErrDegradedMode)
		}},
		{name: "object not found", err: apistatus.ErrObjectNotFound, assertErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
			require.ErrorContains(t, err, "resolve part ID in metabase")
		}},
		{name: "object already removed", err: apistatus.ErrObjectAlreadyRemoved, assertErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
			require.ErrorContains(t, err, "resolve part ID in metabase")
		}},
		{name: "object expired", err: meta.ErrObjectIsExpired, assertErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, meta.ErrObjectIsExpired)
			require.ErrorContains(t, err, "resolve part ID in metabase")
		}},
	} {
		t.Run("metabase/"+tc.name, func(t *testing.T) {
			mdb := mockMetabase{
				resolveECPart: map[resolveECPartKey]resolveECPartValue{
					{cnr: cnr, parent: parentID, pi: pi}: {err: tc.err},
				},
			}

			s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mdb, unimplementedWriteCache{})

			_, err := s.GetECPart(cnr, parentID, pi)
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
				get: map[oid.Address]getValue{
					partAddr: {err: tc.err},
				},
			}

			s := newSimpleTestShard(t, &bs, &mb, nil)

			_, err := s.GetECPart(cnr, parentID, pi)
			require.ErrorIs(t, err, tc.err)
			require.ErrorContains(t, err, fmt.Sprintf("get from BLOB storage by ID %s", partID))
		})
	}

	t.Run("writecache", func(t *testing.T) {
		// TODO: share utility for logger testing
		var lb zaptest.Buffer
		l := zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zap.CombineWriteSyncers(&lb),
			zapcore.DebugLevel,
		))

		// errors
		for _, tc := range []struct {
			name   string
			err    error
			logMsg map[string]any
		}{
			{name: "internal error", err: errors.New("internal error"), logMsg: map[string]any{
				"level":    "info",
				"msg":      "failed to get EC part object from write-cache, trying BLOB storage...",
				"partAddr": partAddr.String(),
				"error":    "internal error",
			}},
			{name: "object not found", err: fmt.Errorf("wrapped: %w", apistatus.ErrObjectNotFound), logMsg: map[string]any{
				"level":    "debug",
				"msg":      "EC part object is missing in write-cache, trying BLOB storage...",
				"partAddr": partAddr.String(),
				"error":    "wrapped: " + apistatus.ErrObjectNotFound.Error(),
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				wc := mockWriteCache{
					get: map[oid.Address]getValue{
						oid.NewAddress(cnr, partID): {err: tc.err},
					},
				}

				s := newSimpleTestShard(t, &bs, &mb, &wc)
				s.log = l

				lb.Reset()

				got, err := s.GetECPart(cnr, parentID, pi)
				require.NoError(t, err)
				require.Equal(t, partObj, got)

				logMsgs := lb.Lines()
				require.Len(t, logMsgs, 1)
				var logMsg map[string]any
				require.NoError(t, json.Unmarshal([]byte(logMsgs[0]), &logMsg))
				require.Subset(t, logMsg, tc.logMsg)
			})
		}

		wc := mockWriteCache{
			get: map[oid.Address]getValue{
				oid.NewAddress(cnr, partID): {obj: partObj},
			},
		}

		s := newSimpleTestShard(t, unimplementedBLOBStore{}, &mb, &wc)

		got, err := s.GetECPart(cnr, parentID, pi)
		require.NoError(t, err)
		require.Equal(t, partObj, got)
	})

	s := newSimpleTestShard(t, &bs, &mb, nil)

	got, err := s.GetECPart(cnr, parentID, pi)
	require.NoError(t, err)
	require.Equal(t, partObj, got)
}
