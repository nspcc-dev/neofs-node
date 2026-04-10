package protoscan

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
)

const (
	uuidLen      = 16
	n3AddressLen = 25
)

// BinaryFieldKind enumerates binary fields of specific format.
type BinaryFieldKind uint8

const (
	_ = iota
	BinaryFieldSHA256
	BinaryFieldN3Address
	BinaryFieldKindUUIDV4
)

// String implements [fmt.Stringer].
func (x BinaryFieldKind) String() string {
	switch x {
	default:
		return "unknown#" + strconv.Itoa(int(x))
	case BinaryFieldSHA256:
		return "SHA256"
	case BinaryFieldN3Address:
		return "N3Address"
	case BinaryFieldKindUUIDV4:
		return "UUIDV4"
	}
}

// verifies b according to its kind. Panics if kind is unknown.
func verifyBinaryField(kind BinaryFieldKind, buffers iprotobuf.BuffersSlice) error {
	switch kind {
	default:
		panic(fmt.Sprintf("unexpected kind %d", kind))
	case BinaryFieldSHA256:
		return verifySHA256(buffers)
	case BinaryFieldN3Address:
		return verifyN3Address(buffers)
	case BinaryFieldKindUUIDV4:
		return verifyUUIDV4(buffers)
	}
}

// verifySHA256 checks whether buffers contain a valid non-zero SHA-256 hash.
func verifySHA256(buffers iprotobuf.BuffersSlice) error {
	// Len() + Contains() may be more optimal generally
	b := buffers.ReadOnlyData()

	if len(b) != sha256.Size {
		return fmt.Errorf("len is %d, expected %d", len(b), sha256.Size)
	}

	if islices.AllZeros(b) {
		return errors.New("all bytes are zero")
	}

	return nil
}

// checks whether buffers contain a valid Neo3 address.
func verifyN3Address(buffers iprotobuf.BuffersSlice) error {
	// TODO: Len() + Byte(n) + Hash(from, to) may be more optimal generally
	b := buffers.ReadOnlyData()

	if len(b) != n3AddressLen {
		return fmt.Errorf("len is %d, expected %d", len(b), n3AddressLen)
	}

	if b[0] != address.NEO3Prefix {
		return fmt.Errorf("prefix byte is 0x%X, expected 0x%X", b[0], address.NEO3Prefix)
	}

	if !bytes.Equal(b[21:], hash.Checksum(b[:21])) {
		return errors.New("checksum mismatch")
	}

	return nil
}

// verifyUUIDV4 checks whether buffers contain a valid UUID V4.
func verifyUUIDV4(buffers iprotobuf.BuffersSlice) error {
	// TODO: Len() + Byte(n) may be more optimal generally
	b := buffers.ReadOnlyData()

	if len(b) != uuidLen {
		return fmt.Errorf("invalid len: %d instead of %d", len(b), uuidLen)
	}

	if ver := b[6] >> 4; ver != 4 {
		return fmt.Errorf("wrong UUID version %d, expected 4", ver)
	}

	return nil
}
