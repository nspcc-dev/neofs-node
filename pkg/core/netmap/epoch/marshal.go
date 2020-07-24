package epoch

import (
	"encoding/binary"
	"io"
)

// Size is a size of Epoch
// in a binary form.
const Size = 8

// Marshal encodes Epoch into a
// binary form and returns the result.
//
// Result slice has Size length.
func Marshal(e Epoch) []byte {
	d := make([]byte, Size)

	binary.BigEndian.PutUint64(d, ToUint64(e))

	return d
}

// UnmarshalBinary unmarshals Epoch from
// a binary representation.
//
// If buffer size is insufficient,
// io.ErrUnexpectedEOF is returned.
func (e *Epoch) UnmarshalBinary(data []byte) error {
	if len(data) < Size {
		return io.ErrUnexpectedEOF
	}

	*e = FromUint64(binary.BigEndian.Uint64(data))

	return nil
}
