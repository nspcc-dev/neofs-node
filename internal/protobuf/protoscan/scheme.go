package protoscan

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// SchemeAlias allows to resolve cross-dependency of messages.
type SchemeAlias = uint8

const (
	_ = iota
	SchemeAliasObjectHeader
)

// resolves scheme by its alias. Panics if alias is unknown.
func resolveScheme(alias SchemeAlias) MessageScheme {
	switch alias {
	default:
		panic(fmt.Sprintf("unexpected alias %d", alias))
	case SchemeAliasObjectHeader:
		return ObjectHeaderScheme
	}
}

// MessageScheme describes scheme of particular NeoFS API protocol message for
// proper scanning.
type MessageScheme struct {
	// Field number -> descriptor mapping.
	Fields map[protowire.Number]MessageField
	// Field of bytes type -> format mapping.
	BinaryFields map[protowire.Number]BinaryFieldKind
	// Field of nested message type -> scheme mapping.
	NestedMessageFields map[protowire.Number]MessageScheme
	// Field of nested message type -> scheme alias mapping.
	NestedMessageAliases map[protowire.Number]SchemeAlias
	// Number of field having same scheme as current message if any.
	RecursiveField protowire.Number
}
