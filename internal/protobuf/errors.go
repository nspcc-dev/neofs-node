package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// TODO: docs.
func NewUnorderedFieldsError(prev, next protowire.Number) error {
	return fmt.Errorf("unordered fields: #%d after #%d", next, prev)
}

// TODO: docs.
func NewWrongFieldTypeError(typ protowire.Type) error {
	return fmt.Errorf("wrong field type %v", typ)
}

// TODO: docs.
func WrapParseFieldTagError(cause error) error {
	return fmt.Errorf("parse field tag: %w", cause)
}

// TODO: docs.
func WrapParseFieldError(num protowire.Number, typ protowire.Type, cause error) error {
	return fmt.Errorf("parse field (#%d,type=%v): %w", num, typ, cause)
}

// TODO: docs.
func WrapSeekFieldError(num protowire.Number, typ protowire.Type, cause error) error {
	return fmt.Errorf("seek field (#%d,type=%v): %w", num, typ, cause)
}
