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
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	uuidLen = 16
)

// fieldType is an enumeration of Protocol Buffers V3 field types used in NeoFS
// API protocol.
type fieldType uint8

// All [fieldType] values supported by this package.
const (
	_ = iota
	fieldTypeUint32
	fieldTypeUint64
	fieldTypeEnum
	fieldTypeBool
	fieldTypeRepeatedEnum
	fieldTypeString
	fieldTypeBytes
	fieldTypeNestedMessage
)

// String implements [fmt.Stringer].
func (x fieldType) String() string {
	switch x {
	case fieldTypeEnum:
		return "enum"
	case fieldTypeUint32:
		return "uint32"
	case fieldTypeUint64:
		return "uint64"
	case fieldTypeBool:
		return "bool"
	case fieldTypeRepeatedEnum:
		return "repeated enum"
	case fieldTypeString:
		return "string"
	case fieldTypeBytes:
		return "bytes"
	case fieldTypeNestedMessage:
		return "nested message"
	default:
		return "unknown#" + strconv.Itoa(int(x))
	}
}

// namedField pairs field name and number.
type namedField struct {
	name string
	typ  fieldType
}

// String implements [fmt.Stringer].
func (x namedField) String() string {
	return x.name + " (" + x.typ.String() + ")"
}

// newNamedField constructs new namedField instance.
func newNamedField(name string, typ fieldType) namedField {
	return namedField{name: name, typ: typ}
}

// newParseFieldError returns common error for failed f's parsing.
func newParseFieldError(f namedField, cause error) error {
	return fmt.Errorf("parse %s field: %w", f, cause)
}

// binaryFieldKind enumerates binary fields of specific format.
type binaryFieldKind uint8

const (
	_ = iota
	binaryFieldSHA256
	binaryFieldN3Address
	binaryFieldKindUUIDV4
)

// String implements [fmt.Stringer].
func (x binaryFieldKind) String() string {
	switch x {
	case binaryFieldSHA256:
		return "SHA256"
	case binaryFieldN3Address:
		return "N3Address"
	case binaryFieldKindUUIDV4:
		return "UUIDV4"
	default:
		return "unknown#" + strconv.Itoa(int(x))
	}
}

// verifySHA256 checks whether buffers contain a valid non-zero SHA-256 hash.
func verifySHA256(buffers iprotobuf.BuffersSlice) error {
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
	b := buffers.ReadOnlyData()
	if len(b) != user.IDSize {
		return fmt.Errorf("len is %d, expected %d", len(b), user.IDSize)
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
	b := buffers.ReadOnlyData()
	if len(b) != uuidLen {
		return fmt.Errorf("invalid len: %d instead of %d", len(b), uuidLen)
	}
	if ver := b[6] >> 4; ver != 4 {
		return fmt.Errorf("wrong UUID version %d, expected 4", ver)
	}
	return nil
}

// verifies b according to its kind. Panics if kind is unknown.
func verifyBinaryField(kind binaryFieldKind, buffers iprotobuf.BuffersSlice) error {
	// TODO: consider optimization to use a byte stream as is instead of slicing
	switch kind {
	case binaryFieldSHA256:
		return verifySHA256(buffers)
	case binaryFieldN3Address:
		return verifyN3Address(buffers)
	case binaryFieldKindUUIDV4:
		return verifyUUIDV4(buffers)
	default:
		panic(fmt.Sprintf("unexpected kind %d", kind))
	}
}

// Scheme alias allows to resolve cross-dependency of messages.
type schemeAlias = uint8

const (
	_ = iota
	schemeAliasObjectHeader
)

// resolves scheme by its alias. Panics if alias is unknown.
func resolveScheme(alias schemeAlias) MessageScheme {
	switch alias {
	default:
		panic(fmt.Sprintf("unexpected alias %d", alias))
	case schemeAliasObjectHeader:
		return ObjectHeaderScheme
	}
}

// MessageScheme describes scheme of particular NeoFS API protocol message for
// proper scanning.
type MessageScheme struct {
	fields           map[protowire.Number]namedField
	binaryKindFields map[protowire.Number]binaryFieldKind
	nestedFields     map[protowire.Number]MessageScheme
	nestedAliases    map[protowire.Number]schemeAlias
	recursionFields  []protowire.Number
}
