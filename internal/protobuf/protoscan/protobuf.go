package protoscan

import "strconv"

// FieldType is an enumeration of Protocol Buffers V3 field types used in NeoFS
// API protocol.
type FieldType uint8

// All [FieldType] values supported by this package.
const (
	_ = iota
	FieldTypeUint32
	FieldTypeUint64
	FieldTypeEnum
	FieldTypeBool
	FieldTypeRepeatedEnum
	FieldTypeString
	FieldTypeBytes
	FeldTypeNestedMessage
)

// String implements [fmt.Stringer].
func (x FieldType) String() string {
	switch x {
	default:
		return "unknown#" + strconv.Itoa(int(x))
	case FieldTypeEnum:
		return "enum"
	case FieldTypeUint32:
		return "uint32"
	case FieldTypeUint64:
		return "uint64"
	case FieldTypeBool:
		return "bool"
	case FieldTypeRepeatedEnum:
		return "repeated enum"
	case FieldTypeString:
		return "string"
	case FieldTypeBytes:
		return "bytes"
	case FeldTypeNestedMessage:
		return "nested message"
	}
}

// MessageField describes message field.
type MessageField struct {
	name string
	typ  FieldType
}

// String implements [fmt.Stringer].
func (x MessageField) String() string {
	return x.name + " (" + x.typ.String() + ")"
}

// NewMessageField is a MessageField constructor.
func NewMessageField(name string, typ FieldType) MessageField {
	return MessageField{name: name, typ: typ}
}
