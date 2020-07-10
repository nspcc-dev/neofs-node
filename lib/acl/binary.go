package acl

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/internal"
)

// BinaryEACLKey is a binary EACL storage key.
type BinaryEACLKey struct {
	cid refs.CID
}

// BinaryEACLValue is a binary EACL storage value.
type BinaryEACLValue struct {
	eacl []byte

	sig []byte
}

// BinaryExtendedACLSource is an interface of storage of binary extended ACL tables with read access.
type BinaryExtendedACLSource interface {
	// Must return binary extended ACL table by key.
	GetBinaryEACL(context.Context, BinaryEACLKey) (BinaryEACLValue, error)
}

// BinaryExtendedACLStore is an interface of storage of binary extended ACL tables.
type BinaryExtendedACLStore interface {
	BinaryExtendedACLSource

	// Must store binary extended ACL table for key.
	PutBinaryEACL(context.Context, BinaryEACLKey, BinaryEACLValue) error
}

// ErrNilBinaryExtendedACLStore is returned by function that expect a non-nil
// BinaryExtendedACLStore, but received nil.
const ErrNilBinaryExtendedACLStore = internal.Error("binary extended ACL store is nil")

const sliceLenSize = 4

var eaclEndianness = binary.BigEndian

// CID is a container ID getter.
func (s BinaryEACLKey) CID() refs.CID {
	return s.cid
}

// SetCID is a container ID setter.
func (s *BinaryEACLKey) SetCID(v refs.CID) {
	s.cid = v
}

// EACL is a binary extended ACL table getter.
func (s BinaryEACLValue) EACL() []byte {
	return s.eacl
}

// SetEACL is a binary extended ACL table setter.
func (s *BinaryEACLValue) SetEACL(v []byte) {
	s.eacl = v
}

// Signature is an EACL signature getter.
func (s BinaryEACLValue) Signature() []byte {
	return s.sig
}

// SetSignature is an EACL signature setter.
func (s *BinaryEACLValue) SetSignature(v []byte) {
	s.sig = v
}

// MarshalBinary returns a binary representation of BinaryEACLValue.
func (s BinaryEACLValue) MarshalBinary() ([]byte, error) {
	data := make([]byte, sliceLenSize+len(s.eacl)+sliceLenSize+len(s.sig))

	off := 0

	eaclEndianness.PutUint32(data[off:], uint32(len(s.eacl)))
	off += sliceLenSize

	off += copy(data[off:], s.eacl)

	eaclEndianness.PutUint32(data[off:], uint32(len(s.sig)))
	off += sliceLenSize

	copy(data[off:], s.sig)

	return data, nil
}

// UnmarshalBinary unmarshals BinaryEACLValue from bytes.
func (s *BinaryEACLValue) UnmarshalBinary(data []byte) (err error) {
	err = io.ErrUnexpectedEOF
	off := 0

	if len(data[off:]) < sliceLenSize {
		return
	}

	aclLn := eaclEndianness.Uint32(data[off:])
	off += 4

	if uint32(len(data[off:])) < aclLn {
		return
	}

	s.eacl = make([]byte, aclLn)
	off += copy(s.eacl, data[off:])

	if len(data[off:]) < sliceLenSize {
		return
	}

	sigLn := eaclEndianness.Uint32(data[off:])
	off += 4

	if uint32(len(data[off:])) < sigLn {
		return
	}

	s.sig = make([]byte, sigLn)
	copy(s.sig, data[off:])

	return nil
}
