package shard

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	iiotest "github.com/nspcc-dev/neofs-node/internal/testutil/iotest"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/storagetest"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestShard_InitPut(t *testing.T) {
	const headerLength = 1 << 10
	header := testutil.RandByteSlice(headerLength)
	const payloadLength = 256 << 10
	payload := testutil.RandByteSlice(payloadLength)

	hdr := objecttest.Object()
	hdr = *hdr.CutPayload()
	hdr.SetPayloadSize(payloadLength)

	addr := hdr.Address()

	writeCacheError := errors.New("any write-cache error")
	blobStorageError := errors.New("any BLOB storage error")
	metabaseError := errors.New("any metabase error")

	mockedMetabaseCountersDiff := meta.CountersDiff{
		Phy:     1,
		Root:    2,
		TS:      3,
		Lock:    4,
		Link:    5,
		GC:      6,
		Payload: 7,
	}

	var mockedMetabase mockMetabaseInitPut
	mockedMetabase.registerOKResult(hdr, mockedMetabaseCountersDiff)

	assertMetricsOnSuccess := func(t *testing.T, mtrc *mockInitPutMetrics) {
		require.Equal(t, map[string]int{
			"phy":  mockedMetabaseCountersDiff.Phy,
			"root": mockedMetabaseCountersDiff.Root,
			"ts":   mockedMetabaseCountersDiff.TS,
			"lock": mockedMetabaseCountersDiff.Lock,
			"link": mockedMetabaseCountersDiff.Link,
			"gc":   mockedMetabaseCountersDiff.GC,
		}, mtrc.typeCounters)
		require.Equal(t, map[string]int64{
			hdr.GetContainerID().String(): mockedMetabaseCountersDiff.Payload,
		}, mtrc.containerCounters)
	}

	assertEmptyMetrics := func(t *testing.T, mtrc mockInitPutMetrics) {
		require.Empty(t, mtrc.typeCounters)
		require.Empty(t, mtrc.containerCounters)
	}

	assertSuccess := func(t *testing.T, sh *Shard, resBuf *bytes.Buffer, resStream *mockWriteCloser, mtrc *mockInitPutMetrics) {
		stream, abortFn, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
		require.NoError(t, err)

		require.True(t, bytes.Equal(resBuf.Bytes(), header))

		var written int
		for chunk := range slices.Chunk(payload, payloadLength/10) {
			n, err := stream.Write(chunk)
			require.NoError(t, err)
			written += n
		}
		require.EqualValues(t, payloadLength, written)

		require.NoError(t, stream.Close())

		require.True(t, resStream.closed)
		require.True(t, bytes.Equal(resBuf.Bytes(), slices.Concat(header, payload)))

		assertMetricsOnSuccess(t, mtrc)

		assertUnlockedMode(t, sh)

		storagetest.AssertWriteStreamAlreadyAborted(t, stream)

		abortFn()

		storagetest.AssertWriteStreamAlreadyAborted(t, stream)
	}

	t.Run("read-only", func(t *testing.T) {
		assert := func(t *testing.T, withWriteCache bool) {
			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

			sh := newSimpleTestShard(t, unimplementedBLOBStore{}, unimplementedMetabase{}, unimplementedWriteCache{},
				WithMode(mode.ReadOnly),
				WithLogger(logger),
			)

			_, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
			require.ErrorIs(t, err, ErrReadOnlyMode)

			assertUnlockedMode(t, sh)

			loggerBuf.AssertEmpty()
		}

		t.Run("write-cache", func(t *testing.T) {
			assert(t, true)
		})

		assert(t, true)
	})

	t.Run("write-cache", func(t *testing.T) {
		t.Run("failure", func(t *testing.T) {
			t.Run("BLOB storage failure", func(t *testing.T) {
				t.Run("init", func(t *testing.T) {
					var wc mockWriteCacheInitPut
					wc.m.registerInitPutErrorResult(addr, headerLength, payloadLength, writeCacheError)

					var bs mockBLOBStoreInitPut
					bs.m.registerInitPutErrorResult(addr, headerLength, payloadLength, blobStorageError)

					var mb unimplementedMetabase // assert not called

					var mtrc mockInitPutMetrics

					logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

					sh := newSimpleTestShard(t, bs, mb, wc,
						WithLogger(logger),
						WithMetricsWriter(&mtrc),
					)

					_, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
					require.ErrorIs(t, err, blobStorageError)
					require.EqualError(t, err, "could not put object to BLOB storage: "+blobStorageError.Error())

					assertUnlockedMode(t, sh)

					assertEmptyMetrics(t, mtrc)

					loggerBuf.AssertSingle(testutil.LogEntry{
						Level:   zap.DebugLevel,
						Message: "can't put object to the write-cache, trying blobstor",
						Fields: map[string]any{
							"error": writeCacheError.Error(),
						},
					})
				})

				t.Run("write", func(t *testing.T) {
					t.Run("init", func(t *testing.T) {
						var wcBuf bytes.Buffer
						wcStream := &mockWriteCloser{
							Writer: iiotest.NewErrorWriterN(&wcBuf, writeCacheError, 1),
						}

						var wcAbortCounter int
						wcAbortFn := func() { wcAbortCounter++ }

						var wc mockWriteCacheInitPut
						wc.m.registerInitPutOKResult(addr, headerLength, payloadLength, wcStream, wcAbortFn)

						var bs mockBLOBStoreInitPut
						bs.m.registerInitPutErrorResult(addr, headerLength, payloadLength, blobStorageError)

						var mb unimplementedMetabase // assert not called

						var mtrc mockInitPutMetrics

						logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

						sh := newSimpleTestShard(t, bs, mb, wc,
							WithLogger(logger),
							WithMetricsWriter(&mtrc),
						)

						stream, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
						require.NoError(t, err)

						require.True(t, bytes.Equal(wcBuf.Bytes(), header))

						_, err = stream.Write(payload)
						require.ErrorIs(t, err, blobStorageError)
						require.EqualError(t, err, "could not put object to BLOB storage: "+blobStorageError.Error())
						require.True(t, bytes.Equal(wcBuf.Bytes(), header))

						assertUnlockedMode(t, sh)

						assertEmptyMetrics(t, mtrc)

						require.Zero(t, wcAbortCounter)

						loggerBuf.AssertSingle(testutil.LogEntry{
							Level:   zap.DebugLevel,
							Message: "can't put object to the write-cache, trying blobstor",
							Fields: map[string]any{
								"error": writeCacheError.Error(),
							},
						})
					})

					var wc mockWriteCacheInitPut
					wc.m.registerInitPutErrorResult(addr, headerLength, payloadLength, writeCacheError)

					var bsBuf bytes.Buffer
					bsStream := &mockWriteCloser{
						Writer: iiotest.NewErrorWriterN(&bsBuf, blobStorageError, 2),
					}

					var bsAbortCounter int
					bsAbortFn := func() { bsAbortCounter++ }

					var bs mockBLOBStoreInitPut
					bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, bsStream, bsAbortFn)

					var mb unimplementedMetabase // assert not called

					var mtrc mockInitPutMetrics

					logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

					sh := newSimpleTestShard(t, bs, mb, wc,
						WithLogger(logger),
						WithMetricsWriter(&mtrc),
					)

					stream, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
					require.NoError(t, err)

					require.True(t, bytes.Equal(bsBuf.Bytes(), header))

					payloadPrefix := payload[:len(payload)/2]
					n, err := stream.Write(payloadPrefix)
					require.NoError(t, err)
					require.EqualValues(t, len(payloadPrefix), n)
					require.True(t, bytes.Equal(bsBuf.Bytes(), slices.Concat(header, payloadPrefix)))

					_, err = stream.Write([]byte{0})
					require.EqualError(t, err, "could not put object to BLOB storage: "+blobStorageError.Error())

					assertUnlockedMode(t, sh)

					assertEmptyMetrics(t, mtrc)

					require.Zero(t, bsAbortCounter)

					loggerBuf.AssertSingle(testutil.LogEntry{
						Level:   zap.DebugLevel,
						Message: "can't put object to the write-cache, trying blobstor",
						Fields: map[string]any{
							"error": writeCacheError.Error(),
						},
					})
				})

				t.Run("close", func(t *testing.T) {
					var wc mockWriteCacheInitPut
					wc.m.registerInitPutErrorResult(addr, headerLength, payloadLength, writeCacheError)

					var bsBuf bytes.Buffer
					bsStream := &mockWriteCloser{
						Writer:     &bsBuf,
						closeError: blobStorageError,
					}

					var bsAbortCounter int
					bsAbortFn := func() { bsAbortCounter++ }

					var bs mockBLOBStoreInitPut
					bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, bsStream, bsAbortFn)

					var mb unimplementedMetabase // assert not called

					var mtrc mockInitPutMetrics

					logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

					sh := newSimpleTestShard(t, bs, mb, wc,
						WithLogger(logger),
						WithMetricsWriter(&mtrc),
					)

					stream, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
					require.NoError(t, err)

					require.True(t, bytes.Equal(bsBuf.Bytes(), header))

					n, err := stream.Write(payload)
					require.NoError(t, err)
					require.EqualValues(t, len(payload), n)
					require.True(t, bytes.Equal(bsBuf.Bytes(), slices.Concat(header, payload)))

					err = stream.Close()
					require.EqualError(t, err, "could not put object to BLOB storage: "+blobStorageError.Error())

					assertUnlockedMode(t, sh)

					assertEmptyMetrics(t, mtrc)

					require.Zero(t, bsAbortCounter)

					loggerBuf.AssertSingle(testutil.LogEntry{
						Level:   zap.DebugLevel,
						Message: "can't put object to the write-cache, trying blobstor",
						Fields: map[string]any{
							"error": writeCacheError.Error(),
						},
					})
				})
			})

			t.Run("write", func(t *testing.T) {
				t.Run("second write failure", func(t *testing.T) {
					var wcBuf bytes.Buffer
					wcStream := &mockWriteCloser{
						Writer: iiotest.NewErrorWriterN(&wcBuf, writeCacheError, 2),
					}

					var wcAbortCounter int
					wcAbortFn := func() { wcAbortCounter++ }

					var wc mockWriteCacheInitPut
					wc.m.registerInitPutOKResult(addr, headerLength, payloadLength, wcStream, wcAbortFn)

					var bsBuf bytes.Buffer
					bsStream := &mockWriteCloser{
						Writer: &bsBuf,
					}

					var bsAbortCounter int
					bsAbortFn := func() { bsAbortCounter++ }

					var bs mockBLOBStoreInitPut
					bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, bsStream, bsAbortFn)

					var mb unimplementedMetabase // assert not called

					var mtrc mockInitPutMetrics

					logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

					sh := newSimpleTestShard(t, bs, mb, wc,
						WithLogger(logger),
						WithMetricsWriter(&mtrc),
					)

					stream, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
					require.NoError(t, err)

					require.True(t, bytes.Equal(wcBuf.Bytes(), header))

					payloadPrefix := payload[:len(payload)/2]
					n, err := stream.Write(payloadPrefix)
					require.NoError(t, err)
					require.EqualValues(t, len(payloadPrefix), n)
					require.True(t, bytes.Equal(wcBuf.Bytes(), slices.Concat(header, payloadPrefix)))
					require.Zero(t, bsBuf.Len())

					_, err = stream.Write([]byte{0})
					require.ErrorIs(t, err, writeCacheError)
					require.EqualError(t, err, "write-cache: "+writeCacheError.Error())
					require.True(t, bytes.Equal(wcBuf.Bytes(), slices.Concat(header, payloadPrefix)))
					require.Zero(t, bsBuf.Len())

					assertUnlockedMode(t, sh)

					assertEmptyMetrics(t, mtrc)

					require.Zero(t, wcAbortCounter)
					require.Zero(t, bsAbortCounter)

					loggerBuf.AssertEmpty()
				})

				var wcBuf bytes.Buffer
				wcStream := &mockWriteCloser{
					Writer: iiotest.NewErrorWriterN(&wcBuf, writeCacheError, 0),
				}

				var wcAbortCounter int
				wcAbortFn := func() { wcAbortCounter++ }

				var wc mockWriteCacheInitPut
				wc.m.registerInitPutOKResult(addr, headerLength, payloadLength, wcStream, wcAbortFn)

				var bsBuf bytes.Buffer
				bsStream := &mockWriteCloser{
					Writer: &bsBuf,
				}

				var bsAbortCounter int
				bsAbortFn := func() { bsAbortCounter++ }

				var bs mockBLOBStoreInitPut
				bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, bsStream, bsAbortFn)

				var mtrc mockInitPutMetrics

				logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

				sh := newSimpleTestShard(t, bs, mockedMetabase, wc,
					WithLogger(logger),
					WithMetricsWriter(&mtrc),
				)

				stream, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
				require.NoError(t, err)

				require.Zero(t, wcBuf.Len())
				require.True(t, bytes.Equal(bsBuf.Bytes(), header))

				n, err := stream.Write(payload)
				require.NoError(t, err)
				require.EqualValues(t, len(payload), n)
				require.True(t, bytes.Equal(bsBuf.Bytes(), slices.Concat(header, payload)))

				err = stream.Close()
				require.NoError(t, err)

				assertUnlockedMode(t, sh)

				assertMetricsOnSuccess(t, &mtrc)

				require.Zero(t, wcAbortCounter)
				require.Zero(t, bsAbortCounter)

				loggerBuf.AssertContains(testutil.LogEntry{
					Level:   zap.DebugLevel,
					Message: "can't put object to the write-cache, trying blobstor",
					Fields: map[string]any{
						"error": writeCacheError.Error(),
					},
				})
				loggerBuf.AssertContains(testutil.LogEntry{
					Level:   zap.InfoLevel,
					Message: "local object storage operation",
					Fields: map[string]any{
						"address": addr.String(),
						"op":      "PUT",
					},
				})
			})

			t.Run("close", func(t *testing.T) {
				t.Run("after write", func(t *testing.T) {
					var wcBuf bytes.Buffer
					wcStream := &mockWriteCloser{
						Writer:     &wcBuf,
						closeError: writeCacheError,
					}

					var wcAbortCounter int
					wcAbortFn := func() { wcAbortCounter++ }

					var wc mockWriteCacheInitPut
					wc.m.registerInitPutOKResult(addr, headerLength, payloadLength, wcStream, wcAbortFn)

					var bs unimplementedBLOBStore // assert not called

					var mb unimplementedMetabase // assert not called

					var mtrc mockInitPutMetrics

					logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

					sh := newSimpleTestShard(t, bs, mb, wc,
						WithLogger(logger),
						WithMetricsWriter(&mtrc),
					)

					stream, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
					require.NoError(t, err)

					require.True(t, bytes.Equal(wcBuf.Bytes(), header))

					n, err := stream.Write(payload)
					require.NoError(t, err)
					require.EqualValues(t, len(payload), n)
					require.True(t, bytes.Equal(wcBuf.Bytes(), slices.Concat(header, payload)))

					err = stream.Close()
					require.EqualError(t, err, "write-cache: "+writeCacheError.Error())
					require.True(t, bytes.Equal(wcBuf.Bytes(), slices.Concat(header, payload)))

					assertUnlockedMode(t, sh)

					assertEmptyMetrics(t, mtrc)

					require.Zero(t, wcAbortCounter)

					loggerBuf.AssertEmpty()
				})

				var wcBuf bytes.Buffer
				wcStream := &mockWriteCloser{
					Writer:     &wcBuf,
					closeError: writeCacheError,
				}

				var wcAbortCounter int
				wcAbortFn := func() { wcAbortCounter++ }

				var wc mockWriteCacheInitPut
				wc.m.registerInitPutOKResult(addr, headerLength, payloadLength, wcStream, wcAbortFn)

				var bsBuf bytes.Buffer
				bsStream := &mockWriteCloser{
					Writer: &bsBuf,
				}

				var bsAbortCounter int
				bsAbortFn := func() { bsAbortCounter++ }

				var bs mockBLOBStoreInitPut
				bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, bsStream, bsAbortFn)

				var mtrc mockInitPutMetrics

				logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

				sh := newSimpleTestShard(t, bs, mockedMetabase, wc,
					WithLogger(logger),
					WithMetricsWriter(&mtrc),
				)

				stream, _, err := sh.InitPut(hdr, headerLength, iiotest.SliceWriterTo(header))
				require.NoError(t, err)

				require.True(t, bytes.Equal(wcBuf.Bytes(), header))
				require.Zero(t, bsBuf.Len())

				err = stream.Close()
				require.NoError(t, err)

				require.True(t, bytes.Equal(bsBuf.Bytes(), header))

				assertUnlockedMode(t, sh)

				assertMetricsOnSuccess(t, &mtrc)

				require.Zero(t, wcAbortCounter)
				require.Zero(t, bsAbortCounter)

				loggerBuf.AssertContains(testutil.LogEntry{
					Level:   zap.DebugLevel,
					Message: "can't put object to the write-cache, trying blobstor",
					Fields: map[string]any{
						"error": writeCacheError.Error(),
					},
				})
				loggerBuf.AssertContains(testutil.LogEntry{
					Level:   zap.InfoLevel,
					Message: "local object storage operation",
					Fields: map[string]any{
						"address": addr.String(),
						"op":      "PUT",
					},
				})
			})
		})

		t.Run("metabase failure", func(t *testing.T) {
			testCommon := func(t *testing.T, wcDeleteErr error, bsDeleteErr error) *testutil.LogBuffer {
				var wcBuf bytes.Buffer
				wcStream := mockWriteCloser{
					Writer: &wcBuf,
				}

				var wcAbortCounter int
				wcAbortFn := func() { wcAbortCounter++ }

				var wc mockWriteCacheInitPut
				wc.m.registerInitPutOKResult(addr, headerLength, payloadLength, &wcStream, wcAbortFn)
				wc.m.registerDeleteResult(addr, wcDeleteErr)

				var bs mockBLOBStoreInitPut
				bs.m.registerDeleteResult(addr, bsDeleteErr)

				var mb mockMetabaseInitPut
				mb.registerErrorResult(hdr, metabaseError)

				var mtrc mockInitPutMetrics

				logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

				sh := newSimpleTestShard(t, bs, mb, wc,
					WithLogger(logger),
					WithMetricsWriter(&mtrc),
				)

				stream, abortFn, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
				require.NoError(t, err)

				require.True(t, bytes.Equal(wcBuf.Bytes(), header))

				n, err := stream.Write(payload)
				require.NoError(t, err)
				require.EqualValues(t, payloadLength, n)

				err = stream.Close()
				require.ErrorIs(t, err, metabaseError)
				require.EqualError(t, err, "could not put object to metabase: "+metabaseError.Error())

				assertUnlockedMode(t, sh)

				require.True(t, bytes.Equal(wcBuf.Bytes(), slices.Concat(header, payload)))
				require.True(t, wcStream.closed)

				require.Zero(t, wcAbortCounter)

				assertEmptyMetrics(t, mtrc)

				storagetest.AssertWriteStreamAlreadyAborted(t, stream)

				abortFn()

				require.Zero(t, wcAbortCounter)

				storagetest.AssertWriteStreamAlreadyAborted(t, stream)

				return loggerBuf
			}

			notFoundErr := fmt.Errorf("some context: %w", apistatus.ErrObjectNotFound)

			t.Run("BLOB storage deletion failure", func(t *testing.T) {
				t.Run("object not found", func(t *testing.T) {
					loggerBuf := testCommon(t, nil, notFoundErr)
					loggerBuf.AssertEmpty()
				})

				loggerBuf := testCommon(t, nil, blobStorageError)
				loggerBuf.AssertContains(testutil.LogEntry{
					Level:   zap.WarnLevel,
					Message: "can't drop object from blobstor on meta put failure",
					Fields: map[string]any{
						"addr":  addr.String(),
						"error": blobStorageError.Error(),
					},
				})
			})

			t.Run("object not found", func(t *testing.T) {
				loggerBuf := testCommon(t, notFoundErr, nil)
				loggerBuf.AssertEmpty()
			})

			loggerBuf := testCommon(t, writeCacheError, nil)
			loggerBuf.AssertContains(testutil.LogEntry{
				Level:   zap.WarnLevel,
				Message: "can't drop object from write cache on meta put failure",
				Fields: map[string]any{
					"addr":  addr.String(),
					"error": writeCacheError.Error(),
				},
			})
		})

		t.Run("abort", func(t *testing.T) {
			testCommon := func(t *testing.T, write bool) {
				var wcBuf bytes.Buffer
				wcStream := mockWriteCloser{
					Writer: &wcBuf,
				}

				var wcAbortCounter int
				wcAbortFn := func() { wcAbortCounter++ }

				var wc mockWriteCacheInitPut
				wc.m.registerInitPutOKResult(addr, headerLength, payloadLength, &wcStream, wcAbortFn)

				var mb unimplementedMetabase // assert not called

				var mtrc mockInitPutMetrics

				logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

				sh := newSimpleTestShard(t, unimplementedBLOBStore{}, mb, wc,
					WithLogger(logger),
					WithMetricsWriter(&mtrc),
				)

				stream, abortFn, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
				require.NoError(t, err)

				require.True(t, bytes.Equal(wcBuf.Bytes(), header))

				if write {
					n, err := stream.Write(payload)
					require.NoError(t, err)
					require.EqualValues(t, payloadLength, n)
					require.True(t, bytes.Equal(wcBuf.Bytes(), slices.Concat(header, payload)))
				}

				abortFn()

				assertUnlockedMode(t, sh)

				require.False(t, wcStream.closed)

				require.EqualValues(t, 1, wcAbortCounter)

				assertEmptyMetrics(t, mtrc)

				loggerBuf.AssertEmpty()

				storagetest.AssertWriteStreamAlreadyAborted(t, stream)
			}

			t.Run("after init", func(t *testing.T) {
				testCommon(t, false)
			})

			t.Run("after write", func(t *testing.T) {
				testCommon(t, true)
			})
		})

		var wcBuf bytes.Buffer
		wcStream := mockWriteCloser{
			Writer: &wcBuf,
		}

		var wc mockWriteCacheInitPut
		wc.m.registerInitPutOKResult(addr, headerLength, payloadLength, &wcStream, nil)

		var mtrc mockInitPutMetrics

		logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

		sh := newSimpleTestShard(t, unimplementedBLOBStore{}, mockedMetabase, wc,
			WithLogger(logger),
			WithMetricsWriter(&mtrc),
		)

		assertSuccess(t, sh, &wcBuf, &wcStream, &mtrc)

		loggerBuf.AssertEmpty()
	})

	t.Run("BLOB storage failure", func(t *testing.T) {
		t.Run("write", func(t *testing.T) {
			t.Run("header", func(t *testing.T) {
				bsStream := mockWriteCloser{
					Writer: iiotest.NewErrorWriter(blobStorageError),
				}

				var bsAbortCounter int
				bsAbortFn := func() { bsAbortCounter++ }

				var bs mockBLOBStoreInitPut
				bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, &bsStream, bsAbortFn)

				var mb unimplementedMetabase // assert not called

				var mtrc mockInitPutMetrics

				logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

				sh := newSimpleTestShard(t, bs, mb, nil,
					WithLogger(logger),
					WithMetricsWriter(&mtrc),
				)

				_, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
				require.ErrorIs(t, err, blobStorageError)
				require.EqualError(t, err, "could not put object to BLOB storage: "+blobStorageError.Error())

				assertUnlockedMode(t, sh)

				require.False(t, bsStream.closed)
				require.Zero(t, bsAbortCounter)

				assertEmptyMetrics(t, mtrc)

				loggerBuf.AssertEmpty()
			})

			var bsBuf bytes.Buffer
			bsStream := mockWriteCloser{
				Writer: iiotest.NewErrorWriterN(&bsBuf, blobStorageError, 1),
			}

			var bsAbortCounter int
			bsAbortFn := func() { bsAbortCounter++ }

			var bs mockBLOBStoreInitPut
			bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, &bsStream, bsAbortFn)

			var mb unimplementedMetabase // assert not called

			var mtrc mockInitPutMetrics

			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

			sh := newSimpleTestShard(t, bs, mb, nil,
				WithLogger(logger),
				WithMetricsWriter(&mtrc),
			)

			stream, abortFn, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
			require.NoError(t, err)

			require.True(t, bytes.Equal(bsBuf.Bytes(), header))

			n, err := stream.Write(payload)
			require.ErrorIs(t, err, blobStorageError)
			require.EqualError(t, err, "could not put object to BLOB storage: "+blobStorageError.Error())
			require.Zero(t, n)

			assertUnlockedMode(t, sh)

			require.False(t, bsStream.closed)
			require.Zero(t, bsAbortCounter)

			assertEmptyMetrics(t, mtrc)

			loggerBuf.AssertEmpty()

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)

			abortFn()

			require.Zero(t, bsAbortCounter)

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)
		})

		t.Run("close", func(t *testing.T) {
			var bsBuf bytes.Buffer
			bsStream := mockWriteCloser{
				Writer:     &bsBuf,
				closeError: blobStorageError,
			}

			var bsAbortCounter int
			bsAbortFn := func() { bsAbortCounter++ }

			var bs mockBLOBStoreInitPut
			bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, &bsStream, bsAbortFn)

			var mb unimplementedMetabase // assert not called

			var mtrc mockInitPutMetrics

			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

			sh := newSimpleTestShard(t, bs, mb, nil,
				WithLogger(logger),
				WithMetricsWriter(&mtrc),
			)

			stream, abortFn, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
			require.NoError(t, err)

			require.True(t, bytes.Equal(bsBuf.Bytes(), header))

			n, err := stream.Write(payload)
			require.NoError(t, err)
			require.EqualValues(t, payloadLength, n)

			err = stream.Close()
			require.ErrorIs(t, err, blobStorageError)
			require.EqualError(t, err, "could not put object to BLOB storage: "+blobStorageError.Error())

			assertUnlockedMode(t, sh)

			require.True(t, bsStream.closed)

			require.Zero(t, bsAbortCounter)

			assertEmptyMetrics(t, mtrc)

			loggerBuf.AssertEmpty()

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)

			abortFn()

			require.Zero(t, bsAbortCounter)

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)
		})

		var bs mockBLOBStoreInitPut
		bs.m.registerInitPutErrorResult(addr, headerLength, payloadLength, blobStorageError)

		var mtrc mockInitPutMetrics

		logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

		sh := newSimpleTestShard(t, bs, unimplementedMetabase{}, nil,
			WithLogger(logger),
			WithMetricsWriter(&mtrc),
		)

		_, _, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
		require.ErrorIs(t, err, blobStorageError)
		require.EqualError(t, err, "could not put object to BLOB storage: "+blobStorageError.Error())

		assertUnlockedMode(t, sh)

		assertEmptyMetrics(t, mtrc)

		loggerBuf.AssertEmpty()
	})

	t.Run("metabase failure", func(t *testing.T) {
		testCommon := func(t *testing.T, bsDeleteErr error) *testutil.LogBuffer {
			var bsBuf bytes.Buffer
			bsStream := mockWriteCloser{
				Writer: &bsBuf,
			}

			var bsAbortCounter int
			bsAbortFn := func() { bsAbortCounter++ }

			var bs mockBLOBStoreInitPut
			bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, &bsStream, bsAbortFn)
			bs.m.registerDeleteResult(addr, bsDeleteErr)

			var mb mockMetabaseInitPut
			mb.registerErrorResult(hdr, metabaseError)

			var mtrc mockInitPutMetrics

			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

			sh := newSimpleTestShard(t, bs, mb, nil,
				WithLogger(logger),
				WithMetricsWriter(&mtrc),
			)

			stream, abortFn, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
			require.NoError(t, err)

			require.True(t, bytes.Equal(bsBuf.Bytes(), header))

			n, err := stream.Write(payload)
			require.NoError(t, err)
			require.EqualValues(t, payloadLength, n)

			err = stream.Close()
			require.ErrorIs(t, err, metabaseError)
			require.EqualError(t, err, "could not put object to metabase: "+metabaseError.Error())

			assertUnlockedMode(t, sh)

			require.True(t, bytes.Equal(bsBuf.Bytes(), slices.Concat(header, payload)))
			require.True(t, bsStream.closed)

			require.Zero(t, bsAbortCounter)

			assertEmptyMetrics(t, mtrc)

			loggerBuf.AssertContains(testutil.LogEntry{
				Level:   zap.InfoLevel,
				Message: "local object storage operation",
				Fields: map[string]any{
					"address": addr.String(),
					"op":      "PUT",
				},
			})

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)

			abortFn()

			require.Zero(t, bsAbortCounter)

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)

			return loggerBuf
		}

		t.Run("BLOB storage deletion failure", func(t *testing.T) {
			loggerBuf := testCommon(t, blobStorageError)

			loggerBuf.AssertContains(testutil.LogEntry{
				Level:   zap.WarnLevel,
				Message: "can't drop object from blobstor on meta put failure",
				Fields: map[string]any{
					"addr":  addr.String(),
					"error": blobStorageError.Error(),
				},
			})
		})

		testCommon(t, nil)
	})

	t.Run("abort", func(t *testing.T) {
		testCommon := func(t *testing.T, write bool) {
			var bsBuf bytes.Buffer
			bsStream := mockWriteCloser{
				Writer: &bsBuf,
			}

			var bsAbortCounter int
			bsAbortFn := func() { bsAbortCounter++ }

			var bs mockBLOBStoreInitPut
			bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, &bsStream, bsAbortFn)

			var mb unimplementedMetabase // assert not called

			var mtrc mockInitPutMetrics

			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

			sh := newSimpleTestShard(t, bs, mb, nil,
				WithLogger(logger),
				WithMetricsWriter(&mtrc),
			)

			stream, abortFn, err := sh.InitPut(hdr, headerLength, bytes.NewBuffer(header))
			require.NoError(t, err)

			require.True(t, bytes.Equal(bsBuf.Bytes(), header))

			if write {
				n, err := stream.Write(payload)
				require.NoError(t, err)
				require.EqualValues(t, payloadLength, n)
				require.True(t, bytes.Equal(bsBuf.Bytes(), slices.Concat(header, payload)))
			}

			abortFn()

			assertUnlockedMode(t, sh)

			require.False(t, bsStream.closed)

			require.EqualValues(t, 1, bsAbortCounter)

			assertEmptyMetrics(t, mtrc)

			loggerBuf.AssertEmpty()

			storagetest.AssertWriteStreamAlreadyAborted(t, stream)
		}

		t.Run("after init", func(t *testing.T) {
			testCommon(t, false)
		})

		t.Run("after write", func(t *testing.T) {
			testCommon(t, true)
		})
	})

	var bsBuf bytes.Buffer
	bsStream := mockWriteCloser{
		Writer: &bsBuf,
	}

	var bsAbortCounter int
	bsAbortFn := func() { bsAbortCounter++ }

	var bs mockBLOBStoreInitPut
	bs.m.registerInitPutOKResult(addr, headerLength, payloadLength, &bsStream, bsAbortFn)

	var mtrc mockInitPutMetrics

	logger, loggerBuf := testutil.NewBufferedLogger(t, zap.InfoLevel)

	sh := newSimpleTestShard(t, bs, mockedMetabase, nil,
		WithLogger(logger),
		WithMetricsWriter(&mtrc),
	)

	assertSuccess(t, sh, &bsBuf, &bsStream, &mtrc)

	require.Zero(t, bsAbortCounter)

	loggerBuf.AssertSingle(testutil.LogEntry{
		Level:   zap.InfoLevel,
		Message: "local object storage operation",
		Fields: map[string]any{
			"address": addr.String(),
			"op":      "PUT",
		},
	})
}

type mockBLOBStoreInitPut struct {
	unimplementedBLOBStore
	m mockInitPut
}

func (x mockBLOBStoreInitPut) InitPut(addr oid.Address, hdrLen uint64, payloadLen uint64, hdrW io.WriterTo) (io.WriteCloser, func(), error) {
	return x.m.InitPut(addr, hdrLen, payloadLen, hdrW)
}

func (x mockBLOBStoreInitPut) Delete(addr oid.Address) error {
	return x.m.Delete(addr)
}

type mockWriteCacheInitPut struct {
	unimplementedWriteCache
	m mockInitPut
}

func (x mockWriteCacheInitPut) InitPut(addr oid.Address, hdrLen uint64, payloadLen uint64, hdrW io.WriterTo) (io.WriteCloser, func(), error) {
	return x.m.InitPut(addr, hdrLen, payloadLen, hdrW)
}

func (x mockWriteCacheInitPut) Delete(addr oid.Address) error {
	return x.m.Delete(addr)
}

type initPutObjectPrm struct {
	address       oid.Address
	headerLength  uint64
	payloadLength uint64
}

type initPutObjectRes struct {
	stream  io.WriteCloser
	abortFn func()
	error   error
}

type mockInitPut struct {
	initPutItems map[initPutObjectPrm]initPutObjectRes
	deleteItems  map[oid.Address]error
}

func (x *mockInitPut) registerInitPutOKResult(addr oid.Address, hdrLen uint64, payloadLen uint64, stream io.WriteCloser, abortFn func()) {
	x._registerInitPutResult(addr, hdrLen, payloadLen, stream, abortFn, nil)
}

func (x *mockInitPut) registerInitPutErrorResult(addr oid.Address, hdrLen uint64, payloadLen uint64, err error) {
	x._registerInitPutResult(addr, hdrLen, payloadLen, nil, nil, err)
}

func (x *mockInitPut) _registerInitPutResult(addr oid.Address, hdrLen uint64, payloadLen uint64, stream io.WriteCloser, abortFn func(), err error) {
	if x.initPutItems == nil {
		x.initPutItems = make(map[initPutObjectPrm]initPutObjectRes)
	}
	x.initPutItems[initPutObjectPrm{
		address:       addr,
		headerLength:  hdrLen,
		payloadLength: payloadLen,
	}] = initPutObjectRes{
		stream:  stream,
		abortFn: abortFn,
		error:   err,
	}
}

func (x mockInitPut) InitPut(addr oid.Address, hdrLen uint64, payloadLen uint64, hdrW io.WriterTo) (io.WriteCloser, func(), error) {
	res, ok := x.initPutItems[initPutObjectPrm{
		address:       addr,
		headerLength:  hdrLen,
		payloadLength: payloadLen,
	}]
	if !ok {
		return nil, nil, errors.New("[test] unknown input")
	}
	if res.error != nil {
		return nil, nil, res.error
	}
	if _, err := hdrW.WriteTo(res.stream); err != nil {
		return nil, nil, err
	}
	return res.stream, res.abortFn, nil
}

func (x *mockInitPut) registerDeleteResult(addr oid.Address, err error) {
	if x.deleteItems == nil {
		x.deleteItems = make(map[oid.Address]error)
	}
	x.deleteItems[addr] = err
}

func (x mockInitPut) Delete(addr oid.Address) error {
	res, ok := x.deleteItems[addr]
	if !ok {
		return errors.New("[test] unknown input")
	}
	return res
}

type mockWriteCloser struct {
	io.Writer
	closed     bool
	closeError error
}

func (x *mockWriteCloser) Close() error {
	x.closed = true
	return x.closeError
}

type mockMetabaseInitPut struct {
	unimplementedMetabase
	objects map[[sha256.Size]byte]metabasePutCountedRes
}

type metabasePutCountedRes struct {
	countersDiff meta.CountersDiff
	error        error
}

func (x *mockMetabaseInitPut) registerOKResult(hdr object.Object, countersDiff meta.CountersDiff) {
	x._registerResult(hdr, countersDiff, nil)
}

func (x *mockMetabaseInitPut) registerErrorResult(hdr object.Object, err error) {
	x._registerResult(hdr, meta.CountersDiff{}, err)
}

func (x *mockMetabaseInitPut) _registerResult(hdr object.Object, countersDiff meta.CountersDiff, err error) {
	if x.objects == nil {
		x.objects = make(map[[sha256.Size]byte]metabasePutCountedRes)
	}
	x.objects[sha256.Sum256(hdr.Marshal())] = metabasePutCountedRes{
		countersDiff: countersDiff,
		error:        err,
	}
}

func (x mockMetabaseInitPut) PutCounted(hdr *object.Object) (meta.CountersDiff, error) {
	if hdr == nil {
		return meta.CountersDiff{}, errors.New("header is nil")
	}
	res, ok := x.objects[sha256.Sum256(hdr.Marshal())]
	if !ok {
		return meta.CountersDiff{}, errors.New("[test] unknown input")
	}
	if res.error != nil {
		return meta.CountersDiff{}, res.error
	}
	return res.countersDiff, nil
}

type mockInitPutMetrics struct {
	unimplementedMetricsWriter
	typeCounters      map[string]int
	containerCounters map[string]int64
}

func (x *mockInitPutMetrics) AddToObjectCounter(typ string, val int) {
	if x.typeCounters == nil {
		x.typeCounters = make(map[string]int)
	}
	x.typeCounters[typ] = val
}

func (x *mockInitPutMetrics) AddToContainerSize(cnr string, val int64) {
	if x.containerCounters == nil {
		x.containerCounters = make(map[string]int64)
	}
	x.containerCounters[cnr] = val
}
