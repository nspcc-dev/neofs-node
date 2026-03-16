package protobuf

import (
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/mem"
)

// MemBuffer is a buffer for some protobuf message. MemBuffer must be
// initialized using [MemBufferPool].
type MemBuffer struct {
	mem.SliceBuffer
	from int
	to   int
	refs atomic.Int32
	pool *sync.Pool
}

// SetBounds seals underlying message with given bounds.
func (x *MemBuffer) SetBounds(from, to int) {
	x.from = from
	x.to = to
}

// Finalize seals underlying message with given bounds and increments ref
// counter for x. Finalize is intended to be called before.
func (x *MemBuffer) Finalize(from, to int) {
	x.SetBounds(from, to)
	x.Ref()
}

// Len returns final length of the underlying message. Len returns undefined
// value before [MemBuffer.SetBounds]/[MemBuffer.Finalize].
//
// Len implements [mem.Buffer].
func (x *MemBuffer) ReadOnlyData() []byte {
	return x.SliceBuffer[x.from:x.to]
}

// Len returns final length of the underlying message. Len returns undefined
// value before [MemBuffer.SetBounds]/[MemBuffer.Finalize].
//
// Len implements [mem.Buffer].
func (x *MemBuffer) Len() int {
	return x.to - x.from
}

// Ref increments ref counter for x.
//
// Ref panics if x has already been freed.
//
// Ref implements [mem.Buffer].
func (x *MemBuffer) Ref() {
	if x.refs.Add(1) <= 1 {
		panic("ref of freed buffer")
	}
}

// Free decrements ref counter for x. If counter reaches zero, x is freed, i.e.
// it is returned back to [MemBufferPool].
//
// Free panics if x has already been freed.
//
// Free implements [mem.Buffer].
func (x *MemBuffer) Free() {
	switch refs := x.refs.Add(-1); {
	case refs > 0:
	case refs == 0:
		x.pool.Put(x)
	default:
		panic("free of freed buffer")
	}
}

// MemBufferPool is a [sync.Pool] for [MemBuffer] instances.
type MemBufferPool struct {
	syncPool *sync.Pool
}

// NewBufferPool constructs MemBufferPool allocating all buffers of a given
// length.
func NewBufferPool(ln int) *MemBufferPool {
	return &MemBufferPool{
		syncPool: &sync.Pool{
			New: func() any {
				return &MemBuffer{
					SliceBuffer: make([]byte, ln),
				}
			},
		},
	}
}

// Get selects MemBuffer from x and returns it.
//
// Instant [MemBuffer.Free] call returns the buffer to x. Use [MemBuffer.Ref] /
// [MemBuffer.Finalize] to prevent release.
func (x *MemBufferPool) Get() *MemBuffer {
	item := x.syncPool.Get().(*MemBuffer)
	item.pool = x.syncPool
	item.refs.Store(1)
	return item
}
