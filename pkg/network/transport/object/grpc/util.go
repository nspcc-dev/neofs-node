package object

import "io"

type bytesReader interface {
	io.Reader
	io.ByteReader
}

type limitedBytesReader struct {
	r bytesReader
	n int
}

func limitBytesReader(r bytesReader, n int) bytesReader { return &limitedBytesReader{r: r, n: n} }

func (x *limitedBytesReader) Read(p []byte) (n int, err error) {
	if x.n <= 0 {
		return 0, io.EOF
	}
	if len(p) > x.n {
		p = p[0:x.n]
	}
	n, err = x.r.Read(p)
	x.n -= n
	return
}

func (x *limitedBytesReader) ReadByte() (byte, error) {
	if x.n <= 0 {
		return 0, io.EOF
	}
	b, err := x.r.ReadByte()
	if err == nil {
		x.n--
	}
	return b, err
}
