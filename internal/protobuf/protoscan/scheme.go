package protoscan

import (
	"google.golang.org/protobuf/encoding/protowire"
)

// MessageScheme describes scheme of particular NeoFS API protocol message for
// proper scanning.
type MessageScheme struct {
	// Field number -> descriptor mapping.
	Fields map[protowire.Number]MessageField
	// Field of bytes type -> format mapping.
	BinaryFields map[protowire.Number]BinaryFieldKind
	// Field of nested message type -> scheme mapping.
	NestedFields map[protowire.Number]MessageScheme
	// Number of field having same scheme as current message if any.
	RecursiveField protowire.Number
}
