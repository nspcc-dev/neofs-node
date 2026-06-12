package fstree

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/zstd"
)

type compressedReader struct {
	*zstd.Decoder
	fileCloser io.Closer
}

func (x compressedReader) Close() error {
	x.Decoder.Close()
	return x.fileCloser.Close()
}

// Seek supports SeekCurrent and positive offsets only, allowing to skip some data.
func (x compressedReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekCurrent || offset < 0 {
		panic("wrong compressed reader seek")
	}
	_, err := io.CopyN(io.Discard, x.Decoder, offset)

	return 0, err
}

type nopReadCloser struct{}

func (nopReadCloser) Read([]byte) (int, error) { return 0, io.EOF }

func (nopReadCloser) Seek(int64, int) (int64, error) { return 0, io.EOF }

func (nopReadCloser) Close() error { return nil }

func checkTooBigRange(off, ln uint64) error {
	if off > math.MaxInt64 || ln > math.MaxInt64 { // 8 exabytes, amply
		return fmt.Errorf("range overflowing int64 is not supported by this server: off=%d,len=%d", off, ln)
	}
	return nil
}

type prefixedReadSeekCloser struct {
	prefix *bytes.Reader
	rest   io.ReadSeekCloser
}

func newPrefixedReadSeekCloser(prefix []byte, f io.ReadSeekCloser) *prefixedReadSeekCloser {
	return &prefixedReadSeekCloser{prefix: bytes.NewReader(prefix), rest: f}
}

func (p *prefixedReadSeekCloser) Read(b []byte) (int, error) {
	var (
		prefBytes = min(len(b), p.prefix.Len())
		n         int
	)

	if prefBytes > 0 {
		k, _ := p.prefix.Read(b[:prefBytes]) // io.EOF can't happen because of prefBytes and bytes.Reader can't have other errors.
		n = k
	}

	k, err := p.rest.Read(b[prefBytes:])
	n += k
	return n, err
}

// Seek supports SeekCurrent and positive offsets only, allowing to skip some data.
func (p *prefixedReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekCurrent || offset < 0 {
		panic("wrong prefixed reader seek")
	}
	var skipBytes = min(int64(p.prefix.Len()), offset)

	_, err := p.prefix.Seek(skipBytes, whence)
	if err != nil {
		return 0, fmt.Errorf("seeking bytes: %w", err)
	}

	return p.rest.Seek(offset-skipBytes, whence)
}

func (p *prefixedReadSeekCloser) Close() error {
	return p.rest.Close()
}

type limitedFileReader struct {
	io.ReadSeekCloser
	limit int64
}

func (l *limitedFileReader) Read(b []byte) (int, error) {
	if l.limit <= 0 {
		return 0, io.EOF
	}
	if int64(len(b)) > l.limit {
		b = b[:int(l.limit)]
	}
	n, err := l.ReadSeekCloser.Read(b)
	l.limit -= int64(n)
	return n, err
}

// Seek supports SeekCurrent and positive offsets only, allowing to skip some data.
func (l *limitedFileReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekCurrent || offset < 0 {
		panic("wrong limited reader seek")
	}
	if offset > l.limit {
		return 0, io.EOF
	}
	_, err := l.ReadSeekCloser.Seek(offset, whence)
	if err == nil {
		l.limit -= offset
	}
	return 0, err
}
