package container

import (
	"encoding/binary"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
)

const (
	saltLenSize = 2

	fixedSize = 0 +
		basic.Size +
		OwnerIDSize +
		saltLenSize
)

// MarshalBinary encodes the container into a binary form
// and returns the result.
func (c *Container) MarshalBinary() ([]byte, error) {
	data := make([]byte, binaryContainerSize(c))

	off := copy(data, basic.Marshal(c.basicACL))

	off += copy(data[off:], c.ownerID.Bytes())

	binary.BigEndian.PutUint16(data[off:], uint16(len(c.salt)))
	off += saltLenSize

	off += copy(data[off:], c.salt)

	if _, err := c.placementRule.MarshalTo(data[off:]); err != nil {
		return nil, err
	}

	return data, nil
}

// UnmarshalBinary unmarshals container from a binary
// representation.
//
// If buffer size is insufficient, io.ErrUnexpectedEOF is returned.
func (c *Container) UnmarshalBinary(data []byte) error {
	if len(data) < binaryContainerSize(c) {
		return io.ErrUnexpectedEOF
	}

	if err := c.basicACL.UnmarshalBinary(data); err != nil {
		return err
	}

	off := basic.Size

	off += copy(c.ownerID[:], data[off:])

	saltLen := binary.BigEndian.Uint16(data[off:])
	off += saltLenSize

	c.salt = make([]byte, saltLen)
	off += copy(c.salt, data[off:])

	if err := c.placementRule.Unmarshal(data[off:]); err != nil {
		return err
	}

	return nil
}

// returns the length of the container in binary form.
func binaryContainerSize(cnr *Container) int {
	return fixedSize +
		len(cnr.salt) +
		cnr.placementRule.Size()
}
