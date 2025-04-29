package shard_test

import (
	"bytes"
	"math"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestShard_GetRange(t *testing.T) {
	t.Run("without write cache", func(t *testing.T) {
		testShardGetRange(t, false)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardGetRange(t, true)
	})
}

func testShardGetRange(t *testing.T, hasWriteCache bool) {
	type testCase struct {
		hasErr      bool
		name        string
		payloadSize int
		rng         *objectSDK.Range
	}

	const (
		writeCacheMaxSize = 1024
		smallObjectSize   = 2048
	)

	newRange := func(off, ln uint64) *objectSDK.Range {
		rng := objectSDK.NewRange()
		rng.SetOffset(off)
		rng.SetLength(ln)
		return rng
	}

	testCases := []testCase{
		{false, "small object, good", 1024, newRange(11, 123)},
		{true, "small object, out of range, big len", 1024, newRange(10, 1020)},
		{true, "small object, out of range, big offset", 1024, newRange(1025, math.MaxUint64-10)},
		{false, "big object, good", 2048, newRange(11, 123)},
		{true, "big object, out of range, big len", 2048, newRange(100, 2000)},
		{true, "big object, out of range, big offset", 2048, newRange(2048, math.MaxUint64-10)},
	}

	if hasWriteCache {
		testCases = append(testCases,
			testCase{false, "object in write-cache, good", 100, newRange(2, 18)},
			testCase{true, "object in write-cache, out of range, big len", 100, newRange(4, 99)},
			testCase{true, "object in write-cache, out of range, big offset", 100, newRange(101, math.MaxUint64-10)})
	}

	sh := newCustomShard(t, t.TempDir(), hasWriteCache,
		[]writecache.Option{writecache.WithMaxObjectSize(writeCacheMaxSize)},
		[]blobstor.Option{blobstor.WithStorages(blobstor.SubStorage{
			Storage: fstree.New(
				fstree.WithPath(filepath.Join(t.TempDir(), "blob"))),
		})})
	defer releaseShard(sh, t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj := generateObject()
			addAttribute(obj, "foo", "bar")
			addPayload(obj, tc.payloadSize)

			addr := object.AddressOf(obj)
			payload := bytes.Clone(obj.Payload())

			err := sh.Put(obj, nil, 0)
			require.NoError(t, err)

			res, err := sh.GetRange(addr, tc.rng.GetOffset(), tc.rng.GetLength(), false)
			if tc.hasErr {
				require.ErrorAs(t, err, &apistatus.ObjectOutOfRange{})
			} else {
				ln := tc.rng.GetLength()
				var to uint64
				if ln != 0 {
					to = tc.rng.GetOffset() + ln
				} else {
					to = uint64(len(obj.Payload()))
				}
				require.Equal(t,
					payload[tc.rng.GetOffset():to],
					res.Payload())
			}
		})
	}
}
