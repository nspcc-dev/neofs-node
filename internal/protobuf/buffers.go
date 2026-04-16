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

// ReadOnlyData returns final length of the underlying message. ReadOnlyData returns undefined
// value before [MemBuffer.SetBounds].
//
// ReadOnlyData implements [mem.Buffer].
func (x *MemBuffer) ReadOnlyData() []byte {
	return x.SliceBuffer[x.from:x.to]
}

// Len returns final length of the underlying message. Len returns undefined
// value before [MemBuffer.SetBounds].
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
// to prevent release.
func (x *MemBufferPool) Get() *MemBuffer {
	item := x.syncPool.Get().(*MemBuffer)
	item.pool = x.syncPool
	item.refs.Store(1)
	return item
}

// BuffersSlice is a wrapper over [mem.BufferSlice] providing protobuf reading interface.
//
// Zero instance is correct: it corresponds to empty data.
type BuffersSlice struct {
	buffers  mem.BufferSlice
	firstOff int
	lastTo   int
}

// NewBuffersSlice initializes BuffersSlice with given buffers.
func NewBuffersSlice(buffers mem.BufferSlice) BuffersSlice {
	if len(buffers) == 0 {
		return BuffersSlice{}
	}

	return BuffersSlice{
		buffers: buffers,
		lastTo:  buffers[len(buffers)-1].Len(),
	}
}

// IsEmpty checks whether x is fully read.
func (x BuffersSlice) IsEmpty() bool {
	for _, b := range x.buffersSeq2 {
		if len(b) > 0 {
			return false
		}
	}
	return true
}

// ReadOnlyData returns read-only buffer of data remaining in x.
func (x *BuffersSlice) ReadOnlyData() []byte {
	if len(x.buffers) == 1 {
		return x.buffers[0].ReadOnlyData()[x.firstOff:x.lastTo]
	}

	var ln int
	for _, b := range x.buffersSeq2 {
		ln += len(b)
	}

	buf := make([]byte, 0, ln)
	for _, b := range x.buffersSeq2 {
		buf = append(buf, b...)
	}
	return buf
}

// MoveNext moves x forward n bytes and returns cut BuffersSlice. If there is
// less than n bytes left, MoveNext returns false.
func (x *BuffersSlice) MoveNext(n int) (BuffersSlice, bool) {
	if len(x.buffers) == 0 {
		return BuffersSlice{}, n == 0
	}

	for i, b := range x.buffersSeq2 {
		ln := len(b)
		if ln < n {
			n -= ln
			continue
		}

		sub := BuffersSlice{
			buffers:  x.buffers[:i+1],
			firstOff: x.firstOff,
			lastTo:   n,
		}
		if len(x.buffers) == 1 {
			sub.lastTo += x.firstOff
		}

		x.buffers = x.buffers[i:]
		x.firstOff = sub.lastTo

		return sub, true
	}

	return BuffersSlice{}, false
}

// bytesSeq is an [iter.Seq] over bytes remaining in x.
func (x *BuffersSlice) bytesSeq(yield func(byte) bool) {
	for i, b := range x.buffersSeq2 {
		for j := range b {
			if !yield(b[j]) {
				if j == len(b)-1 {
					x.buffers = x.buffers[i+1:]
					x.firstOff = 0
				} else if i > 0 {
					x.buffers = x.buffers[i:]
					x.firstOff = j + 1
				} else {
					x.firstOff += j + 1
				}
				return
			}
		}
	}
}

// buffersSeq2 is an [iter.Seq2] over buffers remaining in x.
func (x *BuffersSlice) buffersSeq2(yield func(int, []byte) bool) {
	if len(x.buffers) == 0 {
		return
	}

	if len(x.buffers) == 1 {
		yield(0, x.buffers[0].ReadOnlyData()[x.firstOff:x.lastTo])
		return
	}

	if !yield(0, x.buffers[0].ReadOnlyData()[x.firstOff:]) {
		return
	}

	for i := 1; i < len(x.buffers)-1; i++ {
		if !yield(i, x.buffers[i].ReadOnlyData()) {
			return
		}
	}

	yield(len(x.buffers)-1, x.buffers[len(x.buffers)-1].ReadOnlyData()[:x.lastTo])
}
