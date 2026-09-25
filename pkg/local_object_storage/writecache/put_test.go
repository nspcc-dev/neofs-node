package writecache

import (
	"bytes"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/internal/testutil/fstest"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/storagetest"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const testShardIDString = "EXZsyzcNiL2yK3RXEUjcGK"

var (
	testContainerID = cid.ID{155, 205, 32, 87, 141, 19, 108, 127, 18, 253, 6, 184, 28, 192, 108, 227, 82, 141, 155, 95, 0, 80, 38, 158, 97, 139, 237, 33, 44, 2, 160, 29}
	testObjectID    = oid.ID{195, 62, 227, 206, 13, 60, 235, 227, 21, 40, 86, 14, 111, 228, 7, 43, 110, 203, 223, 147, 41, 184, 41, 85, 84, 31, 168, 145, 195, 78, 118, 36}
	testAddress     = oid.NewAddress(testContainerID, testObjectID)
	// corresponds to testAddress and depth=2.
	testObjectDir = filepath.Join("E", "9")
)

const (
	// corresponds to testAddress and depth=2.
	testObjectFileName = "A82e5awsXzDz4FHEmoz81sVU4c3EQzd1oVM122RVST.BVBcG4LStyX486XkjmwcXytTsiEsed2tPkxEP8USaV4g"
)

func TestCache_InitPut(t *testing.T) {
	const headerLength = 1 << 10
	header := testutil.RandByteSlice(headerLength)
	const payloadLength = 256 << 10
	payload := testutil.RandByteSlice(payloadLength)
	fullData := storagetest.ConcatHeaderAndPayload(header, payload)
	fullLength := uint64(len(fullData))

	t.Run("read-only", func(t *testing.T) {
		c := New(
			WithPath(t.TempDir()),
		)
		require.NoError(t, c.Open(true))

		_, _, err := c.InitPut(oid.Address{}, 0, 0, nil)
		require.ErrorIs(t, err, ErrReadOnly)

		t.Run("metrics", func(t *testing.T) {
			mtrc := newOneTimeInitPutMetric(t, "", fullLength)

			c := New(
				WithPath(t.TempDir()),
				WithMetrics(mtrc),
			)
			require.NoError(t, c.Open(true))

			_, _, err := c.InitPut(oid.Address{}, 0, 0, nil)
			require.ErrorIs(t, err, ErrReadOnly)

			mtrc.assertNoOp()
		})
	})

	t.Run("out of space", func(t *testing.T) {
		c := New(
			WithPath(t.TempDir()),
			WithMaxCacheSize(2),
		)

		_, _, err := c.InitPut(testAddress, 3, 0, nil)
		require.ErrorIs(t, err, ErrOutOfSpace)
		_, _, err = c.InitPut(testAddress, 0, 2, nil)
		require.ErrorIs(t, err, ErrOutOfSpace)

		t.Run("metrics", func(t *testing.T) {
			mtrc := newOneTimeInitPutMetric(t, "", fullLength)

			c := New(
				WithPath(t.TempDir()),
				WithMaxCacheSize(2),
				WithMetrics(mtrc),
			)

			_, _, err := c.InitPut(testAddress, headerLength, payloadLength, nil)
			require.ErrorIs(t, err, ErrOutOfSpace)

			mtrc.assertOp()
			mtrc.assertNoObject()
		})
	})

	assertSingleObjectLogRecord := func(loggerBuf *testutil.LogBuffer) {
		loggerBuf.AssertSingle(testutil.LogEntry{
			Level:   zap.InfoLevel,
			Message: "local object storage operation",
			Fields: map[string]any{
				"shard_id":   testShardIDString,
				"component":  "WriteCache",
				"address":    testAddress.String(),
				"substorage": "write-cache",
				"type":       "write-cache",
				"op":         "PUT",
			},
		})
	}

	t.Run("abort", func(t *testing.T) {
		t.Run("before stream", func(t *testing.T) {
			mtrc := newOneTimeInitPutMetric(t, testShardIDString, fullLength)
			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.InfoLevel)

			c := newInitPutTestCache(t,
				WithMetrics(mtrc),
				WithLogger(logger),
			)

			stream, abortFn, err := c.InitPut(testAddress, headerLength, payloadLength, bytes.NewBuffer(header))
			require.NoError(t, err)

			abortFn()

			fstest.AssertEmptyDir(t, filepath.Join(c.DumpInfo().Path, testObjectDir))

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)

			mtrc.assertOp()
			mtrc.assertNoObject()
			loggerBuf.AssertEmpty()
		})
		t.Run("after write before close", func(t *testing.T) {
			mtrc := newOneTimeInitPutMetric(t, testShardIDString, fullLength)
			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.InfoLevel)

			c := newInitPutTestCache(t,
				WithMetrics(mtrc),
				WithLogger(logger),
			)

			stream, abortFn, err := c.InitPut(testAddress, headerLength, payloadLength, bytes.NewBuffer(header))
			require.NoError(t, err)

			n, err := stream.Write(payload)
			require.NoError(t, err)
			require.EqualValues(t, len(payload), n)

			abortFn()

			fstest.AssertEmptyDir(t, filepath.Join(c.DumpInfo().Path, testObjectDir))

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)

			mtrc.assertOp()
			mtrc.assertNoObject()
			loggerBuf.AssertEmpty()
		})
		t.Run("after stream", func(t *testing.T) {
			mtrc := newOneTimeInitPutMetric(t, testShardIDString, fullLength)
			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.InfoLevel)

			c := newInitPutTestCache(t,
				WithMetrics(mtrc),
				WithLogger(logger),
			)

			stream, abortFn, err := c.InitPut(testAddress, headerLength, payloadLength, bytes.NewBuffer(header))
			require.NoError(t, err)

			n, err := stream.Write(payload)
			require.NoError(t, err)
			require.EqualValues(t, len(payload), n)

			require.NoError(t, stream.Close())

			mtrc.assertOp()
			mtrc.assertObject()
			assertSingleObjectLogRecord(loggerBuf)

			mtrc.reset()

			abortFn()

			fstest.AssertSingleDirFileData(t, filepath.Join(c.DumpInfo().Path, testObjectDir), testObjectFileName, fullData)

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)

			mtrc.assertNoOp()
			assertSingleObjectLogRecord(loggerBuf)
		})
	})

	testOK := func(t *testing.T, opts ...Option) Cache {
		logger, loggerBuf := testutil.NewBufferedLogger(t, zap.InfoLevel)

		opts = append([]Option{
			WithMaxCacheSize(fullLength),
			WithLogger(logger),
		}, opts...)

		c := newInitPutTestCache(t, opts...)

		stream, _, err := c.InitPut(testAddress, headerLength, payloadLength, bytes.NewBuffer(header))
		require.NoError(t, err)

		var written int
		for chunk := range slices.Chunk(payload, len(payload)/10) {
			n, err := stream.Write(chunk)
			require.NoError(t, err)
			require.EqualValues(t, len(chunk), n)
			written += n
		}
		require.EqualValues(t, payloadLength, written)

		require.NoError(t, stream.Close())

		fstest.AssertSingleDirFileData(t, filepath.Join(c.DumpInfo().Path, testObjectDir), testObjectFileName, fullData)

		assertSingleObjectLogRecord(loggerBuf)

		gotData, err := c.GetBytes(testAddress)
		require.NoError(t, err)
		require.True(t, bytes.Equal(gotData, fullData))

		return c
	}

	c := testOK(t)

	// assert volume counter has been increased
	_, _, err := c.InitPut(oidtest.Address(), 1, 0, nil)
	require.ErrorIs(t, err, ErrOutOfSpace)

	t.Run("with metrics", func(t *testing.T) {
		mtrc := newOneTimeInitPutMetric(t, testShardIDString, fullLength)

		c := testOK(t,
			WithMetrics(mtrc),
		)

		mtrc.assertOp()
		mtrc.assertObject()

		mtrc.reset()

		_, _, err := c.InitPut(oidtest.Address(), 1, 0, nil)
		require.ErrorIs(t, err, ErrOutOfSpace)
	})
}

func newInitPutTestCache(t *testing.T, opts ...Option) Cache {
	shardID, err := blobstor.DecodeIDString(testShardIDString)
	require.NoError(t, err)
	opts = append([]Option{WithPath(t.TempDir())}, opts...)
	c := New(opts...)
	require.NoError(t, c.Open(false))
	require.NoError(t, c.Init(shardID))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type oneTimeInitPutMetric struct {
	unimplementedMetrics
	t                    *testing.T
	shardID              string
	objectLength         uint64
	totalInitPutDuration time.Duration
	totalWCSize          uint64
	totalWCObjectCount   uint64
}

func newOneTimeInitPutMetric(t *testing.T, shardID string, objectLen uint64) *oneTimeInitPutMetric {
	return &oneTimeInitPutMetric{
		t:            t,
		shardID:      shardID,
		objectLength: objectLen,
	}
}

func (x *oneTimeInitPutMetric) reset() {
	x.totalInitPutDuration = 0
	x.totalWCSize = 0
	x.totalWCObjectCount = 0
}

func (x oneTimeInitPutMetric) assertOp() {
	require.Positive(x.t, x.totalInitPutDuration)
}

func (x oneTimeInitPutMetric) assertObject() {
	require.Positive(x.t, x.totalWCSize)
	require.Positive(x.t, x.totalWCObjectCount)
}

func (x oneTimeInitPutMetric) assertNoOp() {
	require.Zero(x.t, x.totalInitPutDuration)
	x.assertNoObject()
}

func (x oneTimeInitPutMetric) assertNoObject() {
	require.Zero(x.t, x.totalWCSize)
	require.Zero(x.t, x.totalWCObjectCount)
}

func (x *oneTimeInitPutMetric) AddWCStreamingPutDuration(shardID string, dur time.Duration) {
	require.Equal(x.t, x.shardID, shardID)
	require.Zero(x.t, x.totalInitPutDuration)
	require.Positive(x.t, dur)
	x.totalInitPutDuration += dur
}

func (x *oneTimeInitPutMetric) AddWCSize(shardID string, size uint64) {
	require.Equal(x.t, x.shardID, shardID)
	require.Zero(x.t, x.totalWCSize)
	require.EqualValues(x.t, x.objectLength, size)
	x.totalWCSize++
}

func (x *oneTimeInitPutMetric) IncWCObjectCount(shardID string) {
	require.Equal(x.t, x.shardID, shardID)
	require.Zero(x.t, x.totalWCObjectCount)
	x.totalWCObjectCount++
}
