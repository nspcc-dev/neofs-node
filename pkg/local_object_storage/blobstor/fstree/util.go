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
