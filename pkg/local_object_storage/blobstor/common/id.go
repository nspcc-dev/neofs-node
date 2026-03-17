package common

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
)

// IDSize is the binary shard ID size in bytes.
const IDSize = 16

// ID represents a shard identifier.
type ID struct {
	raw  [IDSize]byte
	str  string
	hash uint64
}

// NewID generates a new shard identifier.
func NewID() (ID, error) {
	uid, err := uuid.NewRandom()
	if err != nil {
		return ID{}, err
	}

	return NewIDFromBytes(uid[:])
}

// NewIDFromBytes constructs ID from fixed-size raw bytes.
func NewIDFromBytes(v []byte) (ID, error) {
	if len(v) != IDSize {
		return ID{}, fmt.Errorf("invalid shard ID length %d, expected %d", len(v), IDSize)
	}

	var id ID
	copy(id.raw[:], v)
	id.str = base58.Encode(id.raw[:])
	id.hash = binary.BigEndian.Uint64(id.raw[:8])

	return id, nil
}

// DecodeIDString decodes base58 string into ID.
func DecodeIDString(s string) (ID, error) {
	if s == "" {
		return ID{}, fmt.Errorf("empty shard ID string")
	}

	b, err := base58.Decode(s)
	if err != nil {
		return ID{}, err
	}

	return NewIDFromBytes(b)
}

// String encodes ID into base58 form.
func (id ID) String() string {
	return id.str
}

// Bytes returns a copy of the raw ID bytes.
func (id ID) Bytes() []byte {
	if id.IsZero() {
		return nil
	}

	return bytes.Clone(id.raw[:])
}

// Equal reports whether two IDs are equal.
func (id ID) Equal(other ID) bool {
	return id == other
}

// Hash returns the cached HRW hash.
func (id ID) Hash() uint64 {
	return id.hash
}

// IsZero reports whether ID is empty.
func (id ID) IsZero() bool {
	return id.str == ""
}
