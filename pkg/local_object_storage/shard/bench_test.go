package shard_test

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

var tests = []struct {
	objSize  uint64
	nThreads int
}{
	{1, 1},
	{1, 20},
	{1, 100},
	{1 << 10, 1},
	{1 << 10, 20},
	{1 << 10, 100},
	{100 << 10, 1},
	{100 << 10, 20},
	{100 << 10, 100},
}

func newTestFSTree(tb testing.TB) common.Storage {
	return fstree.New(
		fstree.WithDepth(4), // Default.
		fstree.WithPath(tb.TempDir()),
		fstree.WithDirNameLen(1), // Default.
		fstree.WithNoSync(false), // Default.
	)
}

func benchmark(b *testing.B, p common.Storage, objSize uint64, nThreads int) {
	data := make([]byte, objSize)
	_, _ = rand.Read(data)

	for b.Loop() {
		var wg sync.WaitGroup

		for range nThreads {
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := p.Put(oidtest.Address(), data)
				require.NoError(b, err)
			}()
		}

		wg.Wait()
	}
}

func BenchmarkPut(b *testing.B) {
	for _, tc := range tests {
		b.Run(fmt.Sprintf("size=%d,thread=%d", tc.objSize, tc.nThreads), func(b *testing.B) {
			for name, creat := range map[string]func(testing.TB) common.Storage{
				"fstree": newTestFSTree,
			} {
				b.Run(name, func(b *testing.B) {
					ptt := creat(b)
					require.NoError(b, ptt.Open(false))
					require.NoError(b, ptt.Init())
					b.Cleanup(func() { _ = ptt.Close() })

					benchmark(b, ptt, tc.objSize, tc.nThreads)
				})
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	const nObjects = 10000

	for _, tc := range tests {
		b.Run(fmt.Sprintf("size=%d,thread=%d", tc.objSize, tc.nThreads), func(b *testing.B) {
			for name, creat := range map[string]func(testing.TB) common.Storage{
				"fstree": newTestFSTree,
			} {
				b.Run(name, func(b *testing.B) {
					ptt, objs := prepareObjects(b, creat, tc.objSize, nObjects)

					b.ResetTimer()
					for n := range b.N {
						var wg sync.WaitGroup

						for i := range tc.nThreads {
							wg.Add(1)
							go func(ind int) {
								defer wg.Done()

								_, err := ptt.Get(objs[nObjects/tc.nThreads*ind+n%(nObjects/tc.nThreads)])
								require.NoError(b, err)
							}(i)
						}

						wg.Wait()
					}
				})
			}
		})
	}
}

func BenchmarkHead(b *testing.B) {
	const nObjects = 10000

	for _, tc := range tests {
		b.Run(fmt.Sprintf("size=%d,thread=%d", tc.objSize, tc.nThreads), func(b *testing.B) {
			for name, creat := range map[string]func(testing.TB) common.Storage{
				"fstree": newTestFSTree,
			} {
				b.Run(name, func(b *testing.B) {
					ptt, objs := prepareObjects(b, creat, tc.objSize, nObjects)

					b.ResetTimer()
					for n := range b.N {
						var wg sync.WaitGroup

						for i := range tc.nThreads {
							wg.Add(1)
							go func(ind int) {
								defer wg.Done()

								_, err := ptt.Head(objs[nObjects/tc.nThreads*ind+n%(nObjects/tc.nThreads)])
								require.NoError(b, err)
							}(i)
						}

						wg.Wait()
					}
				})
			}
		})
	}
}

func prepareObjects(b *testing.B, creat func(testing.TB) common.Storage, objSize, nObjects uint64) (common.Storage, []oid.Address) {
	var objs = make([]oid.Address, 0, nObjects)

	ptt := creat(b)
	require.NoError(b, ptt.Open(false))
	require.NoError(b, ptt.Init())
	b.Cleanup(func() { _ = ptt.Close() })

	obj := object.New()
	data := make([]byte, objSize)
	_, _ = rand.Read(data)
	obj.SetID(oid.ID{1, 2, 3})
	obj.SetContainerID(cid.ID{1, 2, 3})
	obj.SetPayload(data)

	rawData := obj.Marshal()

	var ach = make(chan oid.Address)
	for range 100 {
		go func() {
			for range nObjects / 100 {
				addr := oidtest.Address()
				err := ptt.Put(addr, rawData)
				require.NoError(b, err)
				ach <- addr
			}
		}()
	}
	for range nObjects {
		a := <-ach
		objs = append(objs, a)
	}

	return ptt, objs
}
