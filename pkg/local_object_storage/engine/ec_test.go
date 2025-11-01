package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStorageEngine_GetECPart(t *testing.T) {
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	pi := iec.PartInfo{
		RuleIndex: 123,
		Index:     456,
	}

	t.Run("blocked", func(t *testing.T) {
		s := newEngineWithFixedShardOrder([]shardInterface{unimplementedShard{}}) // to ensure shards are not accessed

		e := errors.New("any error")
		require.NoError(t, s.BlockExecution(e))

		_, _, err := s.GetECPart(cnr, parentID, pi)
		require.Equal(t, e, err)
	})

	var parentObj object.Object
	parentObj.SetContainerID(cnr)
	parentObj.SetID(parentID)

	partObj, err := iec.FormObjectForECPart(neofscryptotest.Signer(), parentObj, testutil.RandByteSlice(32), pi)
	require.NoError(t, err)

	partID := partObj.GetID()
	partAddr := oid.NewAddress(cnr, partID)

	shardOK := &mockShard{
		getECPart: map[getECPartKey]getECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {obj: partObj},
		},
	}

	t.Run("metric", func(t *testing.T) {
		const sleepTime = 50 * time.Millisecond // pretty big for test, pretty fast IRL
		var m testMetrics

		shardOK.eCPartSleep = sleepTime

		s := newEngineWithFixedShardOrder([]shardInterface{shardOK, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.metrics = &m

		_, _, _ = s.GetECPart(cnr, parentID, pi)
		require.GreaterOrEqual(t, time.Duration(m.getECPart.Load()), sleepTime)
	})

	t.Run("zero OID error", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{&mockShard{
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: fmt.Errorf("some error: %w", ierrors.ObjectID{})},
			},
		}, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		require.PanicsWithValue(t, "zero object ID returned as error", func() {
			_, _, _ = s.GetECPart(cnr, parentID, pi)
		})

		lb.AssertEmpty()
	})

	shardAlreadyRemoved := &mockShard{
		getECPart: map[getECPartKey]getECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {err: apistatus.ErrObjectAlreadyRemoved},
		},
	}
	shardExpired := &mockShard{
		getECPart: map[getECPartKey]getECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {err: meta.ErrObjectIsExpired},
		},
	}
	shard500 := &mockShard{
		i: 0,
		getECPart: map[getECPartKey]getECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {err: errors.New("some shard error")},
		},
	}
	shard404 := &mockShard{
		getECPart: map[getECPartKey]getECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {err: apistatus.ErrObjectNotFound},
		},
		getStream: map[getStreamKey]getStreamValue{
			{addr: partAddr, skipMeta: true}: {err: apistatus.ErrObjectNotFound},
		},
	}

	checkOK := func(t *testing.T, s *StorageEngine) {
		hdr, rdr, err := s.GetECPart(cnr, parentID, pi)
		require.NoError(t, err)
		assertGetECPartOK(t, partObj, hdr, rdr)
	}
	checkErrorIs := func(t *testing.T, s *StorageEngine, e error) {
		_, _, err := s.GetECPart(cnr, parentID, pi)
		require.ErrorIs(t, err, e)
	}

	t.Run("404,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardOK})
		s.log = l

		checkOK(t, s)

		lb.AssertEmpty()
	})

	t.Run("404,already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardAlreadyRemoved})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertEmpty()
	})

	t.Run("404,expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardExpired})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	t.Run("404,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	t.Run("internal,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardOK})
		s.log = l

		checkOK(t, s)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to get EC part from shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("0")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardAlreadyRemoved})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to get EC part from shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("0")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardExpired})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to get EC part from shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("0")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to get EC part from shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("0")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("404,OID,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, &mockShard{
			getStream: map[getStreamKey]getStreamValue{
				{addr: partAddr, skipMeta: true}: {obj: partObj},
			},
		}})
		s.log = l

		checkOK(t, s)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID resolved in shard but reading failed, continue bypassing metabase",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		})
	})

	t.Run("404,OID,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID resolved in shard but reading failed, continue bypassing metabase",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		})
	})

	t.Run("404,OID,internal", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, &mockShard{
			i: 2,
			getStream: map[getStreamKey]getStreamValue{
				{addr: partAddr, skipMeta: true}: {err: errors.New("some shard error")},
			},
		}})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEqual([]testutil.LogEntry{{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID resolved in shard but reading failed, continue bypassing metabase",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		}, {
			Level:   zap.InfoLevel,
			Message: "failed to get EC part from shard bypassing metabase, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"partID":    partID.String(),
				"shardID":   base58.Encode([]byte("2")),
				"error":     "some shard error",
			},
		}})
	})

	t.Run("already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shardAlreadyRemoved, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertEmpty()
	})

	t.Run("expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shardExpired, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	for _, tc := range []struct {
		typ       object.Type
		associate func(*object.Object, oid.ID)
	}{
		{typ: object.TypeTombstone, associate: (*object.Object).AssociateDeleted},
		{typ: object.TypeLock, associate: (*object.Object).AssociateLocked},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			const shardNum = 5
			s := testNewEngineWithShardNum(t, shardNum)

			sysObj := *generateObjectWithCID(cnr)
			tc.associate(&sysObj, oidtest.ID())
			sysObj.SetPayload([]byte{})
			addAttribute(&sysObj, "__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(123))

			require.NoError(t, s.Put(&sysObj, nil))

			hdr, rdr, err := s.GetECPart(cnr, sysObj.GetID(), pi)
			require.NoError(t, err)
			assertGetECPartOK(t, sysObj, hdr, rdr)
		})
	}

	l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

	s := newEngineWithFixedShardOrder([]shardInterface{shardOK, unimplementedShard{}}) // to ensure 2nd shard is not accessed
	s.log = l

	checkOK(t, s)

	lb.AssertEmpty()
}

func TestStorageEngine_GetECPartRange(t *testing.T) {
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	pi := iec.PartInfo{
		RuleIndex: 123,
		Index:     456,
	}

	t.Run("too big bounds", func(t *testing.T) {
		s := newEngineWithFixedShardOrder([]shardInterface{unimplementedShard{}}) // to ensure shards are not accessed

		_, _, err := s.GetECPartRange(cnr, parentID, pi, math.MaxInt64+1, 1)
		require.EqualError(t, err, "range overflowing int64 is not supported by this server: off=9223372036854775808,len=1")
		_, _, err = s.GetECPartRange(cnr, parentID, pi, 1, math.MaxInt64+1)
		require.EqualError(t, err, "range overflowing int64 is not supported by this server: off=1,len=9223372036854775808")
	})

	t.Run("blocked", func(t *testing.T) {
		s := newEngineWithFixedShardOrder([]shardInterface{unimplementedShard{}}) // to ensure shards are not accessed

		e := errors.New("any error")
		require.NoError(t, s.BlockExecution(e))

		_, _, err := s.GetECPartRange(cnr, parentID, pi, 0, 1)
		require.Equal(t, e, err)
	})

	var parentObj object.Object
	parentObj.SetContainerID(cnr)
	parentObj.SetID(parentID)

	partObj, err := iec.FormObjectForECPart(neofscryptotest.Signer(), parentObj, testutil.RandByteSlice(32), pi)
	require.NoError(t, err)

	partLen := partObj.PayloadSize()
	off, ln := partLen/3, partLen/2

	partID := partObj.GetID()

	partShardKey := getECPartRangeKey{cnr: cnr, parent: parentID, pi: pi, off: int64(off), ln: int64(ln)}

	shardOK := &mockShard{
		getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
			partShardKey: {obj: partObj},
		},
	}

	t.Run("metric", func(t *testing.T) {
		const sleepTime = 50 * time.Millisecond // pretty big for test, pretty fast IRL
		var m testMetrics

		shardOK.eCPartSleep = sleepTime

		s := newEngineWithFixedShardOrder([]shardInterface{shardOK, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.metrics = &m

		_, _, _ = s.GetECPartRange(cnr, parentID, pi, off, ln)
		require.GreaterOrEqual(t, time.Duration(m.getECPartRange.Load()), sleepTime)
	})

	t.Run("zero OID error", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{&mockShard{
			getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
				partShardKey: {err: fmt.Errorf("some error: %w", ierrors.ObjectID{})},
			},
		}, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		require.PanicsWithValue(t, "zero object ID returned as error", func() {
			_, _, _ = s.GetECPartRange(cnr, parentID, pi, off, ln)
		})

		lb.AssertEmpty()
	})

	shardAlreadyRemoved := &mockShard{
		getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
			partShardKey: {err: apistatus.ErrObjectAlreadyRemoved},
		},
	}
	shardOutOfRange := &mockShard{
		getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
			partShardKey: {err: apistatus.ErrObjectOutOfRange},
		},
	}
	shardExpired := &mockShard{
		getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
			partShardKey: {err: meta.ErrObjectIsExpired},
		},
	}
	shard500 := &mockShard{
		i: 1,
		getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
			partShardKey: {err: errors.New("some shard error")},
		},
	}
	shard404 := &mockShard{
		getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
			partShardKey: {err: apistatus.ErrObjectNotFound},
		},
		getRangeStream: map[getRangeStreamKey]getRangeStreamValue{
			{cnr: cnr, id: partID, off: int64(off), ln: int64(ln)}: {err: apistatus.ErrObjectNotFound},
		},
	}

	checkOK := func(t *testing.T, s *StorageEngine) {
		pldLen, rc, err := s.GetECPartRange(cnr, parentID, pi, off, ln)
		require.NoError(t, err)
		assertGetECPartRangeOK(t, partObj, off, ln, pldLen, rc)
	}
	checkErrorIs := func(t *testing.T, s *StorageEngine, e error) {
		_, _, err := s.GetECPartRange(cnr, parentID, pi, off, ln)
		require.ErrorIs(t, err, e)
	}

	t.Run("404,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardOK, unimplementedShard{}})
		s.log = l

		checkOK(t, s)

		lb.AssertEmpty()
	})

	t.Run("404,already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardAlreadyRemoved, unimplementedShard{}})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertEmpty()
	})

	t.Run("404,out of range", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardOutOfRange, unimplementedShard{}})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectOutOfRange)

		lb.AssertEmpty()
	})

	t.Run("404,expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardExpired, unimplementedShard{}})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	t.Run("404,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	t.Run("internal,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardOK, unimplementedShard{}})
		s.log = l

		checkOK(t, s)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to RANGE EC part in shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardAlreadyRemoved, unimplementedShard{}})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to RANGE EC part in shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardExpired, unimplementedShard{}})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to RANGE EC part in shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to RANGE EC part in shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("404,OID,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
				partShardKey: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, &mockShard{
			getRangeStream: map[getRangeStreamKey]getRangeStreamValue{
				{cnr: cnr, id: partID, off: int64(off), ln: int64(ln)}: {obj: partObj},
			},
		}})
		s.log = l

		checkOK(t, s)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID and payload len resolved in shard but reading failed, continue bypassing metabase",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"partID":    partID.String(),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		})
	})

	t.Run("404,OID,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
				partShardKey: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID and payload len resolved in shard but reading failed, continue bypassing metabase",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"partID":    partID.String(),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		})
	})

	t.Run("404,OID,internal", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			getECPartRange: map[getECPartRangeKey]getECPartRangeValue{
				partShardKey: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, &mockShard{
			i: 2,
			getRangeStream: map[getRangeStreamKey]getRangeStreamValue{
				{cnr: cnr, id: partID, off: int64(off), ln: int64(ln)}: {err: errors.New("some shard error")},
			},
		}})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEqual([]testutil.LogEntry{{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID and payload len resolved in shard but reading failed, continue bypassing metabase",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"partID":    partID.String(),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		}, {
			Level:   zap.InfoLevel,
			Message: "failed to RANGE EC part in shard bypassing metabase, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"partID":    partID.String(),
				"shardID":   base58.Encode([]byte("2")),
				"error":     "some shard error",
			},
		}})
	})

	t.Run("already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shardAlreadyRemoved, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertEmpty()
	})

	t.Run("out of range", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shardOutOfRange, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectOutOfRange)

		lb.AssertEmpty()
	})

	t.Run("expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shardExpired, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	for _, tc := range []struct {
		typ       object.Type
		associate func(*object.Object, oid.ID)
	}{
		{typ: object.TypeTombstone, associate: (*object.Object).AssociateDeleted},
		{typ: object.TypeLock, associate: (*object.Object).AssociateLocked},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			const shardNum = 5
			s := testNewEngineWithShardNum(t, shardNum)

			sysObj := *generateObjectWithCID(cnr)
			tc.associate(&sysObj, oidtest.ID())
			sysObj.SetPayload([]byte{})
			addAttribute(&sysObj, "__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(123))

			require.NoError(t, s.Put(&sysObj, nil))

			gotLen, rc, err := s.GetECPartRange(cnr, sysObj.GetID(), pi, 0, 0)
			require.NoError(t, err)
			assertGetECPartRangeOK(t, sysObj, 0, 0, gotLen, rc)

			_, _, err = s.GetECPartRange(cnr, sysObj.GetID(), pi, 0, 1)
			require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
		})
	}

	l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

	s := newEngineWithFixedShardOrder([]shardInterface{shardOK, unimplementedShard{}}) // to ensure 2nd shard is not accessed
	s.log = l

	checkOK(t, s)

	lb.AssertEmpty()
}

func TestStorageEngine_HeadECPart(t *testing.T) {
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	pi := iec.PartInfo{
		RuleIndex: 123,
		Index:     456,
	}

	t.Run("blocked", func(t *testing.T) {
		s := newEngineWithFixedShardOrder([]shardInterface{unimplementedShard{}}) // to ensure shards are not accessed

		e := errors.New("any error")
		require.NoError(t, s.BlockExecution(e))

		_, err := s.HeadECPart(cnr, parentID, pi)
		require.Equal(t, e, err)
	})

	var parentObj object.Object
	parentObj.SetContainerID(cnr)
	parentObj.SetID(parentID)

	partObj, err := iec.FormObjectForECPart(neofscryptotest.Signer(), parentObj, testutil.RandByteSlice(32), pi)
	require.NoError(t, err)

	partHdr := *partObj.CutPayload()

	partID := partObj.GetID()
	partAddr := oid.NewAddress(cnr, partID)

	shardOK := &mockShard{
		headECPart: map[headECPartKey]headECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {hdr: partHdr},
		},
	}

	t.Run("metric", func(t *testing.T) {
		const sleepTime = 50 * time.Millisecond // pretty big for test, pretty fast IRL
		var m testMetrics

		shardOK.eCPartSleep = sleepTime

		s := newEngineWithFixedShardOrder([]shardInterface{shardOK, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.metrics = &m

		_, _ = s.HeadECPart(cnr, parentID, pi)
		require.GreaterOrEqual(t, time.Duration(m.headECPart.Load()), sleepTime)
	})

	t.Run("zero OID error", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{&mockShard{
			headECPart: map[headECPartKey]headECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: fmt.Errorf("some error: %w", ierrors.ObjectID{})},
			},
		}, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		require.PanicsWithValue(t, "zero object ID returned as error", func() {
			_, _ = s.HeadECPart(cnr, parentID, pi)
		})

		lb.AssertEmpty()
	})

	shardAlreadyRemoved := &mockShard{
		headECPart: map[headECPartKey]headECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {err: apistatus.ErrObjectAlreadyRemoved},
		},
	}
	shardExpired := &mockShard{
		headECPart: map[headECPartKey]headECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {err: meta.ErrObjectIsExpired},
		},
	}
	shard500 := &mockShard{
		i: 0,
		headECPart: map[headECPartKey]headECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {err: errors.New("some shard error")},
		},
	}
	shard404 := &mockShard{
		headECPart: map[headECPartKey]headECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {err: apistatus.ErrObjectNotFound},
		},
		head: map[headKey]headValue{
			{addr: partAddr, raw: true}: {err: apistatus.ErrObjectNotFound},
		},
	}

	checkOK := func(t *testing.T, s *StorageEngine) {
		hdr, err := s.HeadECPart(cnr, parentID, pi)
		require.NoError(t, err)
		require.Equal(t, partHdr, hdr)
	}
	checkErrorIs := func(t *testing.T, s *StorageEngine, e error) {
		_, err := s.HeadECPart(cnr, parentID, pi)
		require.ErrorIs(t, err, e)
	}

	t.Run("404,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardOK})
		s.log = l

		checkOK(t, s)

		lb.AssertEmpty()
	})

	t.Run("404,already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardAlreadyRemoved})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertEmpty()
	})

	t.Run("404,expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shardExpired})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	t.Run("404,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	t.Run("internal,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardOK})
		s.log = l

		checkOK(t, s)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to get EC part header from shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("0")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardAlreadyRemoved})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to get EC part header from shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("0")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shardExpired})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to get EC part header from shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("0")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("internal,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard500, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "failed to get EC part header from shard, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("0")),
				"error":     "some shard error",
			},
		})
	})

	t.Run("404,OID,OK", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			headECPart: map[headECPartKey]headECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, &mockShard{
			head: map[headKey]headValue{
				{addr: partAddr, raw: true}: {hdr: partHdr},
			},
		}})
		s.log = l

		checkOK(t, s)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID resolved in shard but reading failed, continue by ID",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		})
	})

	t.Run("404,OID,404", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			headECPart: map[headECPartKey]headECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, shard404})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID resolved in shard but reading failed, continue by ID",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		})
	})

	t.Run("404,OID,internal", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shard404, &mockShard{
			i: 1,
			headECPart: map[headECPartKey]headECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: fmt.Errorf("some error: %w", ierrors.ObjectID(partID))},
			},
		}, &mockShard{
			i: 2,
			head: map[headKey]headValue{
				{addr: partAddr, raw: true}: {err: errors.New("some shard error")},
			},
		}})
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEqual([]testutil.LogEntry{{
			Level:   zap.InfoLevel,
			Message: "EC part's object ID resolved in shard but reading failed, continue by ID",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"shardID":   base58.Encode([]byte("1")),
				"error":     "some error: " + partID.String(),
			},
		}, {
			Level:   zap.InfoLevel,
			Message: "failed to get EC part header from shard bypassing metabase, ignore error",
			Fields: map[string]any{
				"container": cnr.String(),
				"parent":    parentID.String(),
				"ecRule":    json.Number("123"),
				"partIdx":   json.Number("456"),
				"partID":    partID.String(),
				"shardID":   base58.Encode([]byte("2")),
				"error":     "some shard error",
			},
		}})
	})

	t.Run("already removed", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shardAlreadyRemoved, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectAlreadyRemoved)

		lb.AssertEmpty()
	})

	t.Run("expired", func(t *testing.T) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{shardExpired, unimplementedShard{}}) // to ensure 2nd shard is not accessed
		s.log = l

		checkErrorIs(t, s, apistatus.ErrObjectNotFound)

		lb.AssertEmpty()
	})

	for _, tc := range []struct {
		typ       object.Type
		associate func(*object.Object, oid.ID)
	}{
		{typ: object.TypeTombstone, associate: (*object.Object).AssociateDeleted},
		{typ: object.TypeLock, associate: (*object.Object).AssociateLocked},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			const shardNum = 5
			s := testNewEngineWithShardNum(t, shardNum)

			sysObj := *generateObjectWithCID(cnr)
			tc.associate(&sysObj, oidtest.ID())
			addAttribute(&sysObj, "__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(123))

			require.NoError(t, s.Put(&sysObj, nil))

			hdr, err := s.HeadECPart(cnr, sysObj.GetID(), pi)
			require.NoError(t, err)
			require.Equal(t, sysObj, hdr)
		})
	}

	l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)

	s := newEngineWithFixedShardOrder([]shardInterface{shardOK, unimplementedShard{}}) // to ensure 2nd shard is not accessed
	s.log = l

	checkOK(t, s)

	lb.AssertEmpty()
}

func assertGetECPartOK(t testing.TB, exp, hdr object.Object, rdr io.ReadCloser) {
	b, err := io.ReadAll(rdr)
	require.NoError(t, err)
	hdr.SetPayload(b)
	require.Equal(t, exp, hdr)
}

func assertGetECPartRangeOK(t *testing.T, obj object.Object, off, ln uint64, pldLen uint64, rc io.ReadCloser) {
	require.EqualValues(t, obj.PayloadSize(), pldLen)

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
		ln = pldLen
	}

	require.Equal(t, obj.Payload()[off:][:ln], b)
	require.NoError(t, rc.Close())
}

func testPutTombstoneEC(t *testing.T) {
	const shardNum = 10
	const gcInterval = time.Second

	s := testEngineFromShardOpts(t, shardNum, []shard.Option{
		shard.WithGCRemoverSleepInterval(gcInterval),
	})

	const partNum = 10
	const ruleIdx = 123 // any
	cnr := cidtest.ID()
	signer := neofscryptotest.Signer()

	parent := *generateObjectWithCID(cnr)
	parentAddr := objectcore.AddressOf(&parent)

	var parts []object.Object
	var partAddrs []oid.Address
	for i := range partNum {
		part, err := iec.FormObjectForECPart(signer, parent, testutil.RandByteSlice(32), iec.PartInfo{
			RuleIndex: ruleIdx,
			Index:     i,
		})
		require.NoError(t, err)

		parts = append(parts, part)
		partAddrs = append(partAddrs, objectcore.AddressOf(&part))
	}

	assertSearch := func(t *testing.T, addrs []oid.Address) {
		fs, crs, err := objectcore.PreprocessSearchQuery(nil, nil, "")
		require.NoError(t, err)
		res, newCrs, err := s.Search(cnr, fs, nil, crs, 1000)
		require.NoError(t, err)
		require.Zero(t, newCrs)
		require.Len(t, res, len(addrs))
		for i := range addrs {
			require.Contains(t, res, client.SearchResultItem{ID: addrs[i].Object()})
		}
	}

	assertGetErrors := func(t *testing.T, target error) {
		_, err := s.Get(parentAddr)
		require.ErrorIs(t, err, target)
		_, _, err = s.GetStream(parentAddr)
		require.ErrorIs(t, err, target)
		_, err = s.GetBytes(parentAddr)
		require.ErrorIs(t, err, target)
		_, err = s.GetRange(parentAddr, 0, 1)
		require.ErrorIs(t, err, target)
		_, err = s.Head(parentAddr, false)
		require.ErrorIs(t, err, target)
		_, err = s.Head(parentAddr, true)
		require.ErrorIs(t, err, target)

		for i := range parts {
			_, err := s.Get(partAddrs[i])
			require.ErrorIs(t, err, target)
			_, _, err = s.GetStream(partAddrs[i])
			require.ErrorIs(t, err, target)
			_, err = s.GetBytes(partAddrs[i])
			require.ErrorIs(t, err, target)
			_, err = s.GetRange(partAddrs[i], 0, 1)
			require.ErrorIs(t, err, target)
			_, err = s.Head(partAddrs[i], false)
			require.ErrorIs(t, err, target)
			_, err = s.Head(partAddrs[i], true)
			require.ErrorIs(t, err, target)
			_, _, err = s.GetECPart(cnr, parent.GetID(), iec.PartInfo{RuleIndex: ruleIdx, Index: i})
			require.ErrorIs(t, err, target)
		}
	}

	// before storage
	assertGetErrors(t, apistatus.ErrObjectNotFound)
	assertSearch(t, nil)

	// store parts
	for i := range parts {
		require.NoError(t, s.Put(&parts[i], nil))
	}

	// check before removal
	_, err := s.Get(parentAddr)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	require.ErrorAs(t, err, new(iec.ErrParts))
	_, _, err = s.GetStream(parentAddr)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	require.ErrorAs(t, err, new(iec.ErrParts))
	_, err = s.GetBytes(parentAddr)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	require.ErrorAs(t, err, new(iec.ErrParts))
	_, err = s.GetRange(parentAddr, 0, 1)
	require.ErrorIs(t, err, ierrors.ErrParentObject)
	require.ErrorAs(t, err, new(iec.ErrParts))

	hdr, err := s.Head(parentAddr, false)
	require.NoError(t, err)
	require.Equal(t, parent.CutPayload(), hdr)
	hdr, err = s.Head(parentAddr, true)
	require.NoError(t, err)
	require.Equal(t, parent.CutPayload(), hdr)

	for i := range parts {
		obj, err := s.Get(partAddrs[i])
		require.NoError(t, err)
		require.Equal(t, parts[i], *obj)

		hdr, rdr, err := s.GetStream(partAddrs[i])
		assertGetStreamOK(t, hdr, rdr, err, parts[i])

		b, err := s.GetBytes(partAddrs[i])
		require.NoError(t, err)
		require.Equal(t, parts[i].Marshal(), b)

		const off, ln = 3, 12
		b, err = s.GetRange(partAddrs[i], off, ln)
		require.NoError(t, err)
		require.Equal(t, parts[i].Payload()[off:][:ln], b)

		hdr, err = s.Head(partAddrs[i], false)
		require.NoError(t, err)
		require.Equal(t, parts[i].CutPayload(), hdr)
		hdr, err = s.Head(partAddrs[i], true)
		require.NoError(t, err)
		require.Equal(t, parts[i].CutPayload(), hdr)

		h, rdr, err := s.GetECPart(cnr, parent.GetID(), iec.PartInfo{RuleIndex: ruleIdx, Index: i})
		assertGetStreamOK(t, &h, rdr, err, parts[i])
	}

	assertSearch(t, append(partAddrs, parentAddr))

	// store tombstone
	const tombLastEpoch = 42 // any

	tomb := *generateObjectWithCID(cnr)
	tomb.AssociateDeleted(parent.GetID())
	addAttribute(&tomb, "__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(tombLastEpoch))
	require.NoError(t, s.Put(&tomb, nil))

	// check marked
	assertGetErrors(t, apistatus.ErrObjectAlreadyRemoved)
	assertSearch(t, []oid.Address{objectcore.AddressOf(&tomb)})

	// reach tombstone expiration and check all is gone
	s.HandleNewEpoch(tombLastEpoch + 1)

	require.Eventually(t, func() bool {
		_, err := s.Get(parentAddr)
		return errors.Is(err, apistatus.ErrObjectNotFound)
	}, 2*gcInterval, gcInterval/10)

	assertGetErrors(t, apistatus.ErrObjectNotFound)
	assertSearch(t, nil)
}
