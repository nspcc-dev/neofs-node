package iotest

import "io"

// SliceWriterTo is an [io.WriterTo] writing byte slice into provided
// [io.Writer] once.
//
// SliceWriterTo is useful for multiple [io.WriterTo.WriteTo] calls. For single
// call, it is recommended to use [bytes.Reader] or [bytes.Buffer] instead.
type SliceWriterTo []byte

func (x SliceWriterTo) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(x)
	return int64(n), err
}

type errorWriterN struct {
	w     io.Writer
	err   error
	failN uint32
	curN  uint32
}

// NewErrorWriter returns [io.Writer] always returning (0, err). The err should
// not be nil.
func NewErrorWriter(err error) io.Writer { return &errorWriterN{err: err} }

// NewErrorWriterN returns [io.Writer] returning (0, err) after n calls to w.
func NewErrorWriterN(w io.Writer, err error, n uint32) io.Writer {
	return &errorWriterN{
		w:     w,
		err:   err,
		failN: n,
	}
}

func (x *errorWriterN) Write(p []byte) (int, error) {
	if x.failN == x.curN {
		return 0, x.err
	}
	x.curN++
	return x.w.Write(p)
}
