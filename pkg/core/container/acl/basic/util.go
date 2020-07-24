package basic

import (
	"encoding/binary"
	"io"
)

// Size is a size of ACL
// in a binary form.
const Size = 4

// FromUint32 converts builtin
// uint32 value to ACL.
//
// Try to avoid direct cast for
// better portability.
func FromUint32(v uint32) ACL {
	return ACL(v)
}

// ToUint32 converts ACL value
// to builtin uint32.
//
// Try to avoid direct cast for
// better portability.
func ToUint32(v ACL) uint32 {
	return uint32(v)
}

// Equal reports whether e and e2 are the same ACL.
//
// Function defines the relation of equality
// between two ACL. Try to avoid comparison through
// "==" operator for better portability.
func Equal(a, b ACL) bool {
	return ToUint32(a) == ToUint32(b)
}

// Marshal encodes ACL into a
// binary form and returns the result.
//
// Result slice has Size length.
func Marshal(a ACL) []byte {
	d := make([]byte, Size)

	binary.BigEndian.PutUint32(d, ToUint32(a))

	return d
}

// UnmarshalBinary unmarshals ACL from
// a binary representation.
//
// If buffer size is insufficient,
// io.ErrUnexpectedEOF is returned.
func (a *ACL) UnmarshalBinary(data []byte) error {
	if len(data) < Size {
		return io.ErrUnexpectedEOF
	}

	*a = FromUint32(binary.BigEndian.Uint32(data))

	return nil
}
