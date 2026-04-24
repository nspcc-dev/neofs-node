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
	uuidLen        = 16
	neo3AddressLen = 25
)

// BinaryFieldKind enumerates binary fields of specific format.
type BinaryFieldKind uint8

const (
	_ = iota
	BinaryFieldKindSHA256
	BinaryFieldKindNeo3Address
	BinaryFieldKindUUIDV4
)

// String implements [fmt.Stringer].
func (x BinaryFieldKind) String() string {
	switch x {
	default:
		return "unknown#" + strconv.Itoa(int(x))
	case BinaryFieldKindSHA256:
		return "SHA256"
	case BinaryFieldKindNeo3Address:
		return "Neo3Address"
	case BinaryFieldKindUUIDV4:
		return "UUIDV4"
	}
}

// verifies b according to its kind. Panics if kind is unknown.
func verifyBinaryField(kind BinaryFieldKind, buffers iprotobuf.BuffersSlice) error {
	switch kind {
	default:
		panic(fmt.Sprintf("unexpected binary kind %d", kind))
	case BinaryFieldKindSHA256:
		return verifySHA256(buffers)
	case BinaryFieldKindNeo3Address:
		return verifyNeo3Address(buffers)
	case BinaryFieldKindUUIDV4:
		return verifyUUIDV4(buffers)
	}
}

// verifySHA256 checks whether buffers contain a valid non-zero SHA-256 hash.
func verifySHA256(buffers iprotobuf.BuffersSlice) error {
	// Len() + Contains() may be more optimal generally
	b := buffers.ReadOnlyData()

	if len(b) != sha256.Size {
		return newWrongLenError(len(b), sha256.Size)
	}

	if islices.AllZeros(b) {
		return errors.New("all bytes are zero")
	}

	return nil
}

// checks whether buffers contain a valid Neo3 address.
func verifyNeo3Address(buffers iprotobuf.BuffersSlice) error {
	// TODO: Len() + Byte(n) + Hash(from, to) may be more optimal generally
	b := buffers.ReadOnlyData()

	if len(b) != neo3AddressLen {
		return newWrongLenError(len(b), neo3AddressLen)
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
		return newWrongLenError(len(b), uuidLen)
	}

	if ver := b[6] >> 4; ver != 4 {
		return fmt.Errorf("wrong version %d instead of 4", ver)
	}

	return nil
}
