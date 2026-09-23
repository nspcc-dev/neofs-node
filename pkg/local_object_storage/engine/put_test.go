package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	iiotest "github.com/nspcc-dev/neofs-node/internal/testutil/iotest"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/storagetest"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStorageEngine_PutBinary(t *testing.T) {
	addr := oidtest.Address()

	obj := *generateObjectWithCID(cidtest.ID())
	obj.SetContainerID(addr.Container())
	obj.SetID(addr.Object())

	obj2 := *generateObjectWithCID(cidtest.ID())
	require.NotEqual(t, obj, obj2)
	obj2.SetContainerID(addr.Container())
	obj2.SetID(addr.Object())
	objBin := obj.Marshal()

	e, _, _ := newEngine(t, t.TempDir())

	err := e.Put(context.Background(), &obj, objBin)
	require.NoError(t, err)

	gotObj, err := e.Get(context.Background(), addr)
	require.NoError(t, err)
	require.Equal(t, &obj, gotObj)

	b, err := e.GetBytes(context.Background(), addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)

	// now place some garbage
	addr.SetObject(oidtest.ID())
	obj.SetID(addr.Object()) // to avoid 'already exists' outcome
	invalidObjBin := []byte("definitely not an object")
	err = e.Put(context.Background(), &obj, invalidObjBin)
	require.NoError(t, err)

	b, err = e.GetBytes(context.Background(), addr)
	require.NoError(t, err)
	require.Equal(t, invalidObjBin, b)

	_, err = e.Get(context.Background(), addr)
	require.Error(t, err)
}

func TestStorageEngine_Put_Lock(t *testing.T) {
	for _, shardNum := range []int{1, 5} {
		t.Run("shards="+strconv.Itoa(shardNum), func(t *testing.T) {
			testPutLock(t, shardNum)
		})
	}
}

func testPutLock(t *testing.T, shardNum int) {
	var obj object.Object
	ver := version.Current()
	obj.SetVersion(&ver)
	obj.SetContainerID(cidtest.ID())
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	objID := obj.GetID()
	objAddr := oid.NewAddress(obj.GetContainerID(), objID)

	lock := obj
	lock.SetID(oidtest.OtherID(objID))
	lock.AssociateLocked(objID)

	lockAddr := oid.NewAddress(lock.GetContainerID(), lock.GetID())

	tomb := obj
	tomb.SetID(oidtest.OtherID(objID, lock.GetID()))
	tomb.SetAttributes(
		object.NewAttribute("__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(100)),
	)
	tomb.AssociateDeleted(objID)

	t.Run("non-regular target", func(t *testing.T) {
		for _, typ := range []object.Type{
			object.TypeTombstone,
			object.TypeLock,
			object.TypeLink,
		} {
			s := testNewEngineWithShardNum(t, shardNum)

			obj := obj
			obj.SetType(typ)
			switch typ {
			case object.TypeTombstone:
				obj.AssociateDeleted(oidtest.ID())
			case object.TypeLock:
				obj.AssociateLocked(oidtest.ID())
			default:
			}

			require.NoError(t, s.Put(context.Background(), &obj, nil))

			require.ErrorIs(t, s.Put(context.Background(), &lock, nil), apistatus.ErrLockNonRegularObject)

			locked, err := s.IsLocked(context.Background(), objAddr)
			require.NoError(t, err)
			require.False(t, locked)

			_, err = s.Get(context.Background(), lockAddr)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		}
	})

	t.Run("EC", func(t *testing.T) {
		l, logBuf := testutil.NewBufferedLogger(t, zap.InfoLevel)

		s := testNewEngineWithShardNum(t, shardNum)
		s.log = l

		const partNum = 4
		creator := usertest.User()
		cnr := cidtest.ID()

		var parentObj object.Object
		parentObj.SetContainerID(cnr)
		parentObj.SetOwner(creator.UserID())

		require.NoError(t, parentObj.SetVerificationFields(creator))

		parentID := parentObj.GetID()
		parentAddr := oid.NewAddress(cnr, parentID)

		for i := range partNum {
			partObj, err := iec.FormObjectForECPart(creator, parentObj, testutil.RandByteSlice(32), iec.PartInfo{
				RuleIndex: 1,
				Index:     i,
			})
			require.NoError(t, err)
			require.NoError(t, s.Put(context.Background(), &partObj, nil))
		}

		var lock object.Object
		lock.SetContainerID(parentObj.GetContainerID())
		lock.SetOwner(parentObj.Owner())
		lock.AssociateLocked(parentID)

		require.NoError(t, lock.SetVerificationFields(creator))

		require.NoError(t, s.Put(context.Background(), &lock, nil))

		locked, err := s.IsLocked(context.Background(), parentAddr)
		require.NoError(t, err)
		require.True(t, locked)

		for _, sh := range s.unsortedShards() {
			locked, err := sh.IsLocked(parentAddr)
			require.NoError(t, err)
			require.True(t, locked)
		}

		logBuf.AssertEmpty()
	})

	for _, tc := range []struct {
		name         string
		preset       func(*testing.T, *StorageEngine)
		assertPutErr func(t *testing.T, err error)
	}{
		{name: "no target", preset: func(t *testing.T, s *StorageEngine) {}},
		{name: "with target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(context.Background(), &obj, nil))
		}},
		{name: "with target and tombstone", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(context.Background(), &obj, nil))
			require.NoError(t, s.Put(context.Background(), &tomb, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone without target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(context.Background(), &tomb, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "with target and GC mark", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(context.Background(), &obj, nil))

			err := s.Delete(context.Background(), objAddr, GarbageMarkDefault)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testNewEngineWithShardNum(t, shardNum)

			tc.preset(t, s)

			putErr := s.Put(context.Background(), &lock, nil)
			locked, lockedErr := s.IsLocked(context.Background(), objAddr)
			got, getErr := s.Get(context.Background(), lockAddr)

			if tc.assertPutErr != nil {
				tc.assertPutErr(t, putErr)

				require.NoError(t, lockedErr)
				require.False(t, locked)

				require.ErrorIs(t, getErr, apistatus.ErrObjectNotFound)
			} else {
				require.NoError(t, putErr)

				require.NoError(t, lockedErr)
				require.True(t, locked)

				require.NoError(t, getErr)
				require.Equal(t, lock, *got)
			}
		})
	}
}

func TestStorageEngine_Put_Tombstone(t *testing.T) {
	for _, shardNum := range []int{1, 5} {
		t.Run("shards="+strconv.Itoa(shardNum), func(t *testing.T) {
			testPutTombstone(t, shardNum)
		})
	}

	t.Run("EC", testPutTombstoneEC)
}

func testPutTombstone(t *testing.T, shardNum int) {
	var obj object.Object
	ver := version.Current()
	obj.SetVersion(&ver)
	obj.SetContainerID(cidtest.ID())
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	objID := obj.GetID()
	objAddr := oid.NewAddress(obj.GetContainerID(), objID)

	lock := obj
	lock.SetID(oidtest.OtherID(objID))
	lock.AssociateLocked(objID)

	tomb := obj
	tomb.SetID(oidtest.OtherID(objID, lock.GetID()))
	tomb.SetAttributes(
		object.NewAttribute("__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(100)),
	)
	tomb.AssociateDeleted(objID)

	tombAddr := oid.NewAddress(tomb.GetContainerID(), tomb.GetID())

	for _, tc := range []struct {
		name         string
		preset       func(*testing.T, *StorageEngine)
		assertPutErr func(t *testing.T, err error)
		skip         string
	}{
		{name: "no target", preset: func(t *testing.T, s *StorageEngine) {}},
		{name: "with target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(context.Background(), &obj, nil))
		}},
		{name: "with target and lock", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(context.Background(), &obj, nil))
			require.NoError(t, s.Put(context.Background(), &lock, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectLocked)
		}},
		{name: "lock without target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(context.Background(), &lock, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectLocked)
		}},
		{name: "target is lock", preset: func(t *testing.T, s *StorageEngine) {
			obj := obj
			obj.AssociateLocked(oidtest.ID())
			require.NoError(t, s.Put(context.Background(), &obj, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, meta.ErrLockObjectRemoval)
		}},
		{name: "target is tombstone", preset: func(t *testing.T, s *StorageEngine) {
			obj := obj
			obj.SetAttributes(
				object.NewAttribute("__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(100)),
			)
			obj.AssociateDeleted(oidtest.ID())
			require.NoError(t, s.Put(context.Background(), &obj, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.EqualError(t, err, "could not put object to any shard")
		}, skip: "https://github.com/nspcc-dev/neofs-node/issues/3498"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}

			s := testNewEngineWithShardNum(t, shardNum)

			tc.preset(t, s)

			putTombErr := s.Put(context.Background(), &tomb, nil)
			gotTomb, getTombErr := s.Get(context.Background(), tombAddr)
			_, getObjErr := s.Get(context.Background(), objAddr)

			if tc.assertPutErr != nil {
				tc.assertPutErr(t, putTombErr)

				require.ErrorIs(t, getTombErr, apistatus.ErrObjectNotFound)

				require.NotErrorIs(t, getObjErr, apistatus.ErrObjectAlreadyRemoved)
			} else {
				require.NoError(t, putTombErr)

				require.NoError(t, getTombErr)
				require.Equal(t, tomb, *gotTomb)

				require.ErrorIs(t, getObjErr, apistatus.ErrObjectAlreadyRemoved)
			}
		})
	}
}

func TestShard_InitPut(t *testing.T) {
	ctx := context.Background()
	const headerLength = 1 << 10
	header := testutil.RandByteSlice(headerLength)
	const payloadLength = 256 << 10
	payload := testutil.RandByteSlice(payloadLength)

	hdr := objecttest.Object()
	hdr.SetType(object.TypeRegular)
	hdr = *hdr.CutPayload()
	hdr.SetPayloadSize(payloadLength)

	addr := hdr.Address()

	assertNonEmptyMetrics := func(t *testing.T, mtrc mockInitPutMetrics) {
		require.Len(t, mtrc.existsDurations, 1)
		require.Positive(t, mtrc.existsDurations[0])
		require.Len(t, mtrc.initPutDurations, 1)
		require.Positive(t, mtrc.initPutDurations[0])
	}

	t.Run("invalid type", func(t *testing.T) {
		for _, typ := range []object.Type{
			object.TypeTombstone,
			object.TypeStorageGroup, //nolint:staticcheck
			object.TypeLock,
			object.TypeLink,
			5,
		} {
			t.Run(typ.String(), func(t *testing.T) {
				hdr := hdr
				hdr.SetType(typ)

				var s StorageEngine
				_, _, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
				require.EqualError(t, err, "invalid object type "+typ.String())

				assertUnlockedExecutionBlocker(t, &s)
			})
		}
	})

	t.Run("execution blocked", func(t *testing.T) {
		blockErr := errors.New("any block error")

		s := newEngineWithFixedShardOrder(nil)

		err := s.BlockExecution(blockErr)
		require.NoError(t, err)

		_, _, err = s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		require.ErrorIs(t, err, blockErr)
		require.EqualError(t, err, blockErr.Error())

		assertUnlockedExecutionBlocker(t, s)
	})

	t.Run("existence check failure", func(t *testing.T) {
		t.Run("logic", func(t *testing.T) {
			existsErr := logicerr.Wrap(errors.New("any cause"))

			var sh mockInitPutShard
			sh.registerExistsErrorResult(addr, false, existsErr)

			logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

			s := newEngineWithFixedShardOrder([]shardInterface{&sh})
			s.log = logger

			_, _, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
			require.ErrorIs(t, err, existsErr)
			require.EqualError(t, err, "could not put object to any shard: "+existsErr.Error())

			assertUnlockedExecutionBlocker(t, s)

			loggerBuf.AssertContains(testutil.LogEntry{
				Level:   zap.WarnLevel,
				Message: "could not check existence of object in shard",
				Fields: map[string]any{
					"shard_id": "",
					"error":    existsErr.Error(),
				},
			})
			loggerBuf.AssertContains(testutil.LogEntry{
				Level:   zap.WarnLevel,
				Message: "object put: check object existence",
				Fields: map[string]any{
					"addr":  addr.String(),
					"shard": "",
					"error": existsErr.Error(),
				},
			})
		})

		existsErr := errors.New("any exists error")

		var sh mockInitPutShard
		sh.registerExistsErrorResult(addr, false, existsErr)

		s := newEngineWithFixedShardOrder([]shardInterface{&sh})

		_, _, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		require.ErrorIs(t, err, existsErr)
		require.EqualError(t, err, "could not put object to any shard: "+existsErr.Error())

		assertUnlockedExecutionBlocker(t, s)
	})

	testExistsRes := func(t *testing.T, exists bool, existsErr error) {
		var sh mockInitPutShard
		sh._registerExistsResult(addr, false, exists, existsErr)

		var mtrc mockInitPutMetrics

		logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{&sh})
		s.metrics = &mtrc
		s.log = logger

		_, _, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		if exists {
			require.ErrorIs(t, err, ierrors.ErrObjectExists)
			require.EqualError(t, err, ierrors.ErrObjectExists.Error())
		} else {
			require.ErrorIs(t, err, existsErr)
			require.EqualError(t, err, existsErr.Error())
		}

		assertUnlockedExecutionBlocker(t, s)

		assertNonEmptyMetrics(t, mtrc)

		loggerBuf.AssertEmpty()
	}

	t.Run("already removed", func(t *testing.T) {
		testExistsRes(t, false, fmt.Errorf("some context: %w", apistatus.ErrObjectAlreadyRemoved))
	})

	t.Run("parent object", func(t *testing.T) {
		err := ierrors.NewParentObjectError(errors.New("any cause"))
		testExistsRes(t, false, fmt.Errorf("some context: %w", err))
	})

	t.Run("not found", func(t *testing.T) {
		testExistsRes(t, false, fmt.Errorf("some context: %w", apistatus.ErrObjectNotFound))
	})

	t.Run("expired", func(t *testing.T) {
		testExistsRes(t, true, fmt.Errorf("some context: %w", meta.ErrObjectIsExpired))
	})

	t.Run("exists", func(t *testing.T) {
		testExistsRes(t, true, nil)
	})

	t.Run("no shards", func(t *testing.T) {
		s := newEngineWithFixedShardOrder(nil)

		_, _, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		require.EqualError(t, err, "could not put object to any shard: no shards")

		assertUnlockedExecutionBlocker(t, s)
	})

	t.Run("EC sorting", func(t *testing.T) {
		hdr := hdr
		hdr.SetParentID(oidtest.ID())
		addAttribute(&hdr, object.AttributeECPrefix+"any", "value")

		s := New()

		var sorted bool
		s.sortShardsFn = func(_ *StorageEngine, id oid.ID) []shardWrapper {
			if sorted {
				require.Equal(t, hdr.GetParentID(), id)
			} else {
				require.Equal(t, hdr.GetID(), id)
			}
			sorted = true
			return nil
		}

		_, _, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		require.True(t, sorted)
		require.EqualError(t, err, "could not put object to any shard: no shards")

		assertUnlockedExecutionBlocker(t, s)
	})

	t.Run("init failure", func(t *testing.T) {
		newShardWithError := func(err error) *mockInitPutShard {
			var sh mockInitPutShard
			sh.registerExistsOKResult(addr, false, false)
			sh.registerInitPutErrorResult(hdr, headerLength, err)
			return &sh
		}

		otherErr := errors.New("other error")

		shs := []shardInterface{
			newShardWithError(shard.ErrReadOnlyMode),
			newShardWithError(common.ErrReadOnly),
			newShardWithError(common.ErrNoSpace),
			newShardWithError(otherErr),
		}

		var mtrc mockInitPutMetrics

		logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder(shs)
		s.metrics = &mtrc
		s.log = logger

		_, _, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		require.ErrorContains(t, err, "could not put object to any shard")

		assertUnlockedExecutionBlocker(t, s)

		assertNonEmptyMetrics(t, mtrc)

		loggerBuf.AssertContains(testutil.LogEntry{
			Level:   zap.WarnLevel,
			Message: "could not put object to shard",
			Fields: map[string]any{
				"error":    "logical error: shard is in read-only mode",
				"shard_id": "",
			},
		})
		loggerBuf.AssertContains(testutil.LogEntry{
			Level:   zap.WarnLevel,
			Message: "could not put object to shard",
			Fields: map[string]any{
				"error":    "logical error: opened as read-only",
				"shard_id": "",
			},
		})
		loggerBuf.AssertContains(testutil.LogEntry{
			Level:   zap.WarnLevel,
			Message: "could not put object to shard",
			Fields: map[string]any{
				"error":    "logical error: no free space",
				"shard_id": "",
			},
		})
		loggerBuf.AssertContains(testutil.LogEntry{
			Level:   zap.WarnLevel,
			Message: "could not put object to shard",
			Fields: map[string]any{
				"error":    "logical error: no free space",
				"shard_id": "",
			},
		})
		loggerBuf.AssertContains(testutil.LogEntry{
			Level:   zap.WarnLevel,
			Message: "could not put object to shard",
			Fields: map[string]any{
				"error":       otherErr.Error(),
				"error count": json.Number("1"),
				"shard_id":    "",
			},
		})
	})

	t.Run("write failure", func(t *testing.T) {
		writeErr := errors.New("any write error")

		var buf bytes.Buffer
		resStream := mockWriteCloser{
			Writer: iiotest.NewErrorWriterN(&buf, writeErr, 2), // header + first chunk
		}

		var abortCallCount uint
		abortFn := func() { abortCallCount++ }

		var sh mockInitPutShard
		sh.registerExistsOKResult(addr, false, false)
		sh.registerInitPutOKResult(hdr, headerLength, &resStream, abortFn)

		var mtrc mockInitPutMetrics

		logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{&sh})
		s.metrics = &mtrc
		s.log = logger

		stream, abortFn, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		require.NoError(t, err)

		assertUnlockedExecutionBlocker(t, s)

		require.True(t, bytes.Equal(buf.Bytes(), header))

		n, err := stream.Write(payload)
		require.NoError(t, err)
		require.EqualValues(t, payloadLength, n)

		require.True(t, bytes.Equal(buf.Bytes(), slices.Concat(header, payload)))

		_, err = stream.Write([]byte{0})
		require.ErrorIs(t, err, writeErr)
		require.EqualError(t, err, "could not put object to any shard: "+writeErr.Error())

		require.False(t, resStream.closed)

		require.True(t, bytes.Equal(buf.Bytes(), slices.Concat(header, payload)))

		require.Zero(t, abortCallCount)

		assertNonEmptyMetrics(t, mtrc)

		storagetest.AssertWriteStreamAlreadyAborted(t, stream)

		abortFn()

		storagetest.AssertWriteStreamAlreadyAborted(t, stream)

		require.Zero(t, abortCallCount)

		loggerBuf.AssertSingle(testutil.LogEntry{
			Level:   zap.WarnLevel,
			Message: "could not put object to shard",
			Fields: map[string]any{
				"error":       "any write error",
				"error count": json.Number("1"),
				"shard_id":    "",
			},
		})
	})

	t.Run("close failure", func(t *testing.T) {
		closeErr := errors.New("any close error")

		var buf bytes.Buffer
		resStream := mockWriteCloser{
			Writer:     &buf,
			closeError: closeErr,
		}

		var abortCallCount uint
		abortFn := func() { abortCallCount++ }

		var sh mockInitPutShard
		sh.registerExistsOKResult(addr, false, false)
		sh.registerInitPutOKResult(hdr, headerLength, &resStream, abortFn)

		var mtrc mockInitPutMetrics

		logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{&sh})
		s.metrics = &mtrc
		s.log = logger

		stream, abortFn, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		require.NoError(t, err)

		assertUnlockedExecutionBlocker(t, s)

		require.True(t, bytes.Equal(buf.Bytes(), header))

		n, err := stream.Write(payload)
		require.NoError(t, err)
		require.EqualValues(t, payloadLength, n)

		require.True(t, bytes.Equal(buf.Bytes(), slices.Concat(header, payload)))

		err = stream.Close()
		require.ErrorIs(t, err, closeErr)
		require.EqualError(t, err, "could not put object to any shard: "+closeErr.Error())

		require.True(t, resStream.closed)

		require.True(t, bytes.Equal(buf.Bytes(), slices.Concat(header, payload)))

		require.Zero(t, abortCallCount)

		assertNonEmptyMetrics(t, mtrc)

		storagetest.AssertWriteStreamAlreadyAborted(t, stream)

		abortFn()

		storagetest.AssertWriteStreamAlreadyAborted(t, stream)

		require.Zero(t, abortCallCount)

		loggerBuf.AssertSingle(testutil.LogEntry{
			Level:   zap.WarnLevel,
			Message: "could not put object to shard",
			Fields: map[string]any{
				"error":       "any close error",
				"error count": json.Number("1"),
				"shard_id":    "",
			},
		})
	})

	t.Run("abort", func(t *testing.T) {
		var buf bytes.Buffer
		resStream := mockWriteCloser{
			Writer: &buf,
		}

		var abortCallCount uint
		abortFn := func() { abortCallCount++ }

		var sh mockInitPutShard
		sh.registerExistsOKResult(addr, false, false)
		sh.registerInitPutOKResult(hdr, headerLength, &resStream, abortFn)

		var mtrc mockInitPutMetrics

		logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

		s := newEngineWithFixedShardOrder([]shardInterface{&sh})
		s.metrics = &mtrc
		s.log = logger

		stream, abortFn, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
		require.NoError(t, err)

		assertUnlockedExecutionBlocker(t, s)

		require.True(t, bytes.Equal(buf.Bytes(), header))

		n, err := stream.Write(payload)
		require.NoError(t, err)
		require.EqualValues(t, payloadLength, n)

		require.True(t, bytes.Equal(buf.Bytes(), slices.Concat(header, payload)))

		abortFn()

		require.False(t, resStream.closed)

		require.True(t, bytes.Equal(buf.Bytes(), slices.Concat(header, payload)))

		require.EqualValues(t, 1, abortCallCount)

		assertNonEmptyMetrics(t, mtrc)

		storagetest.AssertWriteStreamAlreadyAborted(t, stream)

		abortFn()

		storagetest.AssertWriteStreamAlreadyAborted(t, stream)

		require.EqualValues(t, 1, abortCallCount)

		loggerBuf.AssertEmpty()
	})

	var buf bytes.Buffer
	resStream := mockWriteCloser{
		Writer: &buf,
	}

	var abortCallCount uint
	abortFn := func() { abortCallCount++ }

	var sh mockInitPutShard
	sh.registerExistsOKResult(addr, false, false)
	sh.registerInitPutOKResult(hdr, headerLength, &resStream, abortFn)

	var mtrc mockInitPutMetrics

	logger, loggerBuf := testutil.NewBufferedLogger(t, zap.DebugLevel)

	s := newEngineWithFixedShardOrder([]shardInterface{&sh})
	s.metrics = &mtrc
	s.log = logger

	stream, abortFn, err := s.InitPut(ctx, hdr, headerLength, bytes.NewReader(header))
	require.NoError(t, err)

	assertUnlockedExecutionBlocker(t, s)

	require.True(t, bytes.Equal(buf.Bytes(), header))

	var written int
	for chunk := range slices.Chunk(payload, payloadLength/10) {
		n, err := stream.Write(chunk)
		require.NoError(t, err)
		written += n
	}
	require.EqualValues(t, payloadLength, written)

	require.NoError(t, stream.Close())

	require.True(t, resStream.closed)

	require.True(t, bytes.Equal(buf.Bytes(), slices.Concat(header, payload)))

	assertNonEmptyMetrics(t, mtrc)

	storagetest.AssertWriteStreamAlreadyAborted(t, stream)

	abortFn()

	storagetest.AssertWriteStreamAlreadyAborted(t, stream)

	require.Zero(t, abortCallCount)

	loggerBuf.AssertEmpty()
}

type shardExistsPrm struct {
	address          oid.Address
	ignoreExpiration bool
}

type shardExistsRes struct {
	exists bool
	error  error
}

type shardInitPutPrm struct {
	hdrHash      [sha256.Size]byte
	headerLength uint64
}

type shardInitPutRes struct {
	stream  io.WriteCloser
	abortFn func()
	error   error
}

type mockInitPutShard struct {
	unimplementedShard
	existsItems  map[shardExistsPrm]shardExistsRes
	initPutItems map[shardInitPutPrm]shardInitPutRes
}

func (x *mockInitPutShard) registerExistsOKResult(addr oid.Address, ignoreExpiration bool, exists bool) {
	x._registerExistsResult(addr, ignoreExpiration, exists, nil)
}

func (x *mockInitPutShard) registerExistsErrorResult(addr oid.Address, ignoreExpiration bool, err error) {
	x._registerExistsResult(addr, ignoreExpiration, false, err)
}

func (x *mockInitPutShard) _registerExistsResult(addr oid.Address, ignoreExpiration bool, exists bool, err error) {
	if x.existsItems == nil {
		x.existsItems = make(map[shardExistsPrm]shardExistsRes)
	}
	x.existsItems[shardExistsPrm{
		address:          addr,
		ignoreExpiration: ignoreExpiration,
	}] = shardExistsRes{
		exists: exists,
		error:  err,
	}
}

func (x *mockInitPutShard) Exists(addr oid.Address, ignoreExpiration bool) (bool, error) {
	res, ok := x.existsItems[shardExistsPrm{
		address:          addr,
		ignoreExpiration: ignoreExpiration,
	}]
	if !ok {
		return false, errors.New("[test] unknown object requested")
	}
	return res.exists, res.error
}

func (x *mockInitPutShard) registerInitPutOKResult(hdr object.Object, hdrLen uint64, stream io.WriteCloser, abortFn func()) {
	x._registerInitPutResult(hdr, hdrLen, stream, abortFn, nil)
}

func (x *mockInitPutShard) registerInitPutErrorResult(hdr object.Object, hdrLen uint64, err error) {
	x._registerInitPutResult(hdr, hdrLen, nil, nil, err)
}

func newShardInitPutPrm(hdr object.Object, hdrLen uint64) shardInitPutPrm {
	return shardInitPutPrm{
		hdrHash:      sha256.Sum256(hdr.Marshal()),
		headerLength: hdrLen,
	}
}

func (x *mockInitPutShard) _registerInitPutResult(hdr object.Object, hdrLen uint64, stream io.WriteCloser, abortFn func(), err error) {
	if x.initPutItems == nil {
		x.initPutItems = make(map[shardInitPutPrm]shardInitPutRes)
	}
	x.initPutItems[newShardInitPutPrm(hdr, hdrLen)] = shardInitPutRes{
		stream:  stream,
		abortFn: abortFn,
		error:   err,
	}
}

func (x mockInitPutShard) InitPut(hdr object.Object, hdrLen uint64, hdrW io.WriterTo) (io.WriteCloser, func(), error) {
	res, ok := x.initPutItems[newShardInitPutPrm(hdr, hdrLen)]
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

type mockInitPutMetrics struct {
	unimplementedMetrics
	existsDurations  []time.Duration
	initPutDurations []time.Duration
}

func (x *mockInitPutMetrics) AddExistsDuration(d time.Duration) {
	x.existsDurations = append(x.existsDurations, d)
}

func (x *mockInitPutMetrics) AddStreamingPutDuration(d time.Duration) {
	x.initPutDurations = append(x.initPutDurations, d)
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
