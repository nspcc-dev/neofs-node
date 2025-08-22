package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
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

		shardOK.getECPartSleep = sleepTime

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
