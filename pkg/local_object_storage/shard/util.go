package shard

import "io"

// TODO: utilize in internal space.
type readCloser struct {
	io.Reader
	io.Closer
}

func limitReadCloser(rc io.ReadCloser, n int64) io.ReadCloser {
	return readCloser{
		Reader: io.LimitReader(rc, n),
		Closer: rc,
	}
}
