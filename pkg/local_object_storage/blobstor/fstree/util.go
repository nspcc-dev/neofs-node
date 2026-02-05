package fstree

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

type nopReadCloser struct{}

func (nopReadCloser) Read([]byte) (int, error) { return 0, io.EOF }

func (nopReadCloser) Close() error { return nil }

type zstdStream struct {
	*zstd.Decoder
	src io.ReadCloser
}

func (x zstdStream) Close() error {
	x.Decoder.Close()
	return x.src.Close()
}
