package io

import "io"

// BuffersWriter is the interface that wraps the basic WriteBuffers method.
//
// WriteBuffers sequentially writes all L bytes from bs to the underlying data
// stream. It returns the number of bytes written from p (0 <= n <= L) and any
// error encountered that caused the write to stop early. WriteBuffers must
// return a non-nil error if it returns n < L. WriteBuffers must not bs and its
// elements, even temporarily.
//
// Implementations must not retain bs and its elements.
type BuffersWriter interface {
	WriteBuffers(bs [][]byte) (int, error)
}

// BuffersWriteCloser is the interface that groups io.WriteCloser and
// BuffersWriter interfaces.
type BuffersWriteCloser interface {
	io.WriteCloser
	BuffersWriter
}
