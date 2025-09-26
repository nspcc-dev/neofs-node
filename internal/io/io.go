package io

import (
	"bytes"
	"io"
)

// ReadCloser is an [io.ReadCloser] build from [io.Reader] and [io.Closer].
type ReadCloser struct {
	io.Reader
	io.Closer
}

// LimitReadCloser makes rc to read at most n bytes.
func LimitReadCloser(rc io.ReadCloser, n int64) io.ReadCloser {
	return ReadCloser{
		Reader: io.LimitReader(rc, n),
		Closer: rc,
	}
}

// PrefixReadCloser prepends rc with given prefix.
func PrefixReadCloser(rc io.ReadCloser, prefix []byte) io.ReadCloser {
	return ReadCloser{
		Reader: io.MultiReader(bytes.NewReader(prefix), rc),
		Closer: rc,
	}
}
