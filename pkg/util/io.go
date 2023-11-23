package util

import (
	"bytes"
	"io"
)

type nopCloser struct{ io.ReadSeeker }

func (nopCloser) Close() error { return nil }

// NewBytesReadSeekCloser returns [io.ReadSeekCloser] working over b with
// no-op [io.Closer]. If Close is not needed, [bytes.NewReader] should be used.
func NewBytesReadSeekCloser(b []byte) io.ReadSeekCloser {
	return nopCloser{bytes.NewReader(b)}
}
