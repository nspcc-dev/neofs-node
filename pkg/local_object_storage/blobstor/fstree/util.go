package fstree

import (
	"io"
)

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
