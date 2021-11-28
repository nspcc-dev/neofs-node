package internal

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"
)

// StringifySubnetClientGroupID returns string representation of SubnetClientGroupID using MarshalText.
// Returns string with message on error.
func StringifySubnetClientGroupID(id *SubnetClientGroupID) string {
	text, err := id.MarshalText()
	if err != nil {
		return fmt.Sprintf("<invalid> %v", err)
	}

	return string(text)
}

// MarshalText encodes SubnetClientGroupID into text format according to NeoFS API V2 protocol:
// value in base-10 integer string format.
//
// Implements encoding.TextMarshaler.
func (x *SubnetClientGroupID) MarshalText() ([]byte, error) {
	num := x.GetValue() // NPE safe, returns zero on nil

	return []byte(strconv.FormatUint(uint64(num), 10)), nil
}

// UnmarshalText decodes SubnetID from the text according to NeoFS API V2 protocol:
// should be base-10 integer string format with bitsize = 32.
//
// Returns strconv.ErrRange if integer overflows uint32.
//
// Must not be called on nil.
//
// Implements encoding.TextUnmarshaler.
func (x *SubnetClientGroupID) UnmarshalText(txt []byte) error {
	num, err := strconv.ParseUint(string(txt), 10, 32)
	if err != nil {
		return fmt.Errorf("invalid numeric value: %w", err)
	}

	x.SetNumber(uint32(num))

	return nil
}

// Marshal encodes SubnetClientGroupID into a binary format of NeoFS API V2 protocol
// (Protocol Buffers with direct field order).
func (x *SubnetClientGroupID) Marshal() ([]byte, error) {
	return proto.Marshal(x)
}

// Unmarshal decodes SubnetClientGroupID from NeoFS API V2 binary format (see Marshal). Must not be called on nil.
func (x *SubnetClientGroupID) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, x)
}

// SetNumber sets SubnetClientGroupID value in uint32 format. Must not be called on nil.
// By default, number is 0.
func (x *SubnetClientGroupID) SetNumber(num uint32) {
	x.Value = num
}
