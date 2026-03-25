package protobuf

import (
	"hash"
	"io"
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

// TODO: docs.
type BuffersSlice struct {
	buffers mem.BufferSlice
	curOff  int
	lastTo  int
}

func (x *BuffersSlice) Reset(buffers mem.BufferSlice) {
	x.buffers = buffers
	x.curOff = 0
	x.lastTo = buffers[len(buffers)-1].Len()
}

// TODO: docs.
func NewBuffersSlice(buffers mem.BufferSlice) BuffersSlice {
	if len(buffers) == 0 {
		return BuffersSlice{}
	}

	return BuffersSlice{
		buffers: buffers,
		curOff:  0,
		lastTo:  buffers[len(buffers)-1].Len(),
	}
}

func (x BuffersSlice) IsEmpty() bool {
	return x.Len() == 0
}

func (x *BuffersSlice) buffersSeq(yield func([]byte) bool) {
	if len(x.buffers) == 0 {
		return
	}

	if len(x.buffers) == 1 {
		yield(x.buffers[0].ReadOnlyData()[x.curOff:x.lastTo])
		return
	}

	if !yield(x.buffers[0].ReadOnlyData()[x.curOff:]) {
		return
	}

	for i := range len(x.buffers) - 2 {
		if !yield(x.buffers[i+1].ReadOnlyData()) {
			return
		}
	}

	yield(x.buffers[len(x.buffers)-1].ReadOnlyData()[:x.lastTo])
}

func (x *BuffersSlice) bytesSeq(yield func(byte) bool) {
	var buf []byte
	for {
		buf = x.buffers[0].ReadOnlyData()
		if len(x.buffers) == 1 {
			buf = buf[:x.lastTo]
		}

		for x.curOff < len(buf) {
			cnt := yield(buf[x.curOff])
			x.curOff++
			if !cnt {
				if x.curOff == len(buf) {
					x.buffers = x.buffers[1:]
					x.curOff = 0
				}
				return
			}
		}

		x.buffers = x.buffers[1:]
		x.curOff = 0

		if x.IsEmpty() {
			return
		}
	}
}

func (x *BuffersSlice) MoveNext(n int) (BuffersSlice, error) {
	if len(x.buffers) == 0 {
		if n > 0 {
			return BuffersSlice{}, io.ErrUnexpectedEOF
		}
		return BuffersSlice{}, nil
	}

	sub := *x
	var ln int

	for i := 0; ; i++ {
		if i == 0 {
			if len(x.buffers) == 1 {
				ln = x.lastTo - x.curOff
			} else {
				ln = x.buffers[0].Len() - x.curOff
			}
		} else if i < len(x.buffers)-1 {
			ln = x.buffers[i].Len()
		} else {
			ln = x.lastTo
		}

		if n > ln {
			if i == len(x.buffers)-1 {
				break
			}
			n -= ln
			continue
		}

		if n < ln {
			x.buffers = x.buffers[i:]
			if i == 0 {
				x.curOff += n
			} else {
				x.curOff = 0
			}
		} else {
			x.buffers = x.buffers[i+1:]
			x.curOff = 0
		}

		sub.buffers = sub.buffers[:i+1]
		sub.lastTo = n
		if i == 0 {
			sub.lastTo += sub.curOff
		}
		return sub, nil
	}

	return BuffersSlice{}, io.ErrUnexpectedEOF
}

// TODO: docs.
func (x *BuffersSlice) ReadOnlyData() []byte {
	if len(x.buffers) == 0 {
		return nil
	}

	if len(x.buffers) == 1 {
		return x.buffers[0].ReadOnlyData()[x.curOff:x.lastTo]
	}

	buf := make([]byte, x.Len())
	return buf[:x.CopyTo(buf)]
}

func (x BuffersSlice) Len() int {
	if len(x.buffers) == 0 {
		return 0
	}

	var ln int
	for buf := range x.buffersSeq {
		ln += len(buf)
	}

	return ln
}

func (x BuffersSlice) HashTo(h hash.Hash) {
	if len(x.buffers) == 0 {
		return
	}

	for buf := range x.buffersSeq {
		h.Write(buf)
	}
}

func (x BuffersSlice) CopyTo(dst []byte) int {
	if len(x.buffers) == 0 {
		return 0
	}
	var n int
	for buf := range x.buffersSeq {
		n += copy(dst[n:], buf)
	}
	return n
}
