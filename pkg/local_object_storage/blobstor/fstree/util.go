package fstree

import (
	"fmt"
	"io"
	"math"
)

type readerCloser struct {
	io.Reader
	io.Closer
}
type readerTwoClosers struct {
	io.ReadCloser
	baseCloser io.Closer
}

func (x readerTwoClosers) Close() error {
	x.ReadCloser.Close()
	return x.baseCloser.Close()
}

type nopReadCloser struct{}

func (nopReadCloser) Read([]byte) (int, error) { return 0, io.EOF }

func (nopReadCloser) Close() error { return nil }

func verifyRequestedRange(off, ln uint64) error {
	if ln == 0 && off != 0 {
		return fmt.Errorf("invalid range off=%d,ln=0", off)
	}
	return nil
}

func checkPayloadBounds(pldLen, off, ln uint64) bool {
	return off < pldLen && pldLen-off >= ln
}

func checkTooBigRange(off, ln uint64) error {
	if off > math.MaxInt64 || ln > math.MaxInt64 { // 8 exabytes, amply
		return fmt.Errorf("range overflowing int64 is not supported by this server: off=%d,len=%d", off, ln)
	}
	return nil
}
