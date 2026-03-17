package shard

import (
	"bytes"

	"github.com/mr-tron/base58"
)

// ID represents shard identifier.
type ID []byte

// NewFromBytes constructs ID from byte slice.
func NewFromBytes(v []byte) *ID {
	id := ID(bytes.Clone(v))
	return &id
}

// DecodeString decodes base58 string into ID.
func DecodeString(s string) (*ID, error) {
	if s == "" {
		return nil, nil
	}

	b, err := base58.Decode(s)
	if err != nil {
		return nil, err
	}

	return NewFromBytes(b), nil
}

// String encodes ID into base58 form.
func (id ID) String() string {
	return base58.Encode(id)
}

// Bytes returns a copy of the raw ID bytes.
func (id ID) Bytes() []byte {
	return bytes.Clone(id)
}

// Equal reports whether two IDs are equal.
func (id ID) Equal(other *ID) bool {
	if other == nil {
		return false
	}

	return bytes.Equal(id, *other)
}

// IsZero reports whether ID is empty.
func (id ID) IsZero() bool {
	return len(id) == 0
}
