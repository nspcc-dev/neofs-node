package util

import (
	"io"
)

// SaltXOR xors bits of data with salt
// repeating salt if necessary.
func SaltXOR(data, salt []byte) []byte {
	return SaltXOROffset(data, salt, 0)
}

// SaltXOROffset xors bits of data with salt starting from off byte
// repeating salt if necessary.
func SaltXOROffset(data, salt []byte, off int) (result []byte) {
	result = make([]byte, len(data))
	ls := len(salt)
	if ls == 0 {
		copy(result, data)
		return
	}

	for i := range result {
		result[i] = data[i] ^ salt[(i+off)%ls]
	}
	return
}

type saltWriter struct {
	w io.Writer

	off int

	salt []byte
}

// NewSaltingWriter returns io.Writer instance that applies
// salt to written data and write the result to w.
func NewSaltingWriter(w io.Writer, salt []byte) io.Writer {
	if len(salt) == 0 {
		return w
	}

	return &saltWriter{
		w:    w,
		salt: salt,
	}
}

func (w *saltWriter) Write(data []byte) (int, error) {
	if dataLen := len(data); dataLen > 0 {
		data = SaltXOROffset(data, w.salt, w.off)
		w.off += dataLen
	}

	return w.w.Write(data)
}
