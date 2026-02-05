package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// NewUnorderedFieldsError returns common error for field order violation when
// field #n2 goes after #n1.
func NewUnorderedFieldsError(n1, n2 protowire.Number) error {
	return fmt.Errorf("unordered fields: #%d after #%d", n2, n1)
}

// NewRepeatedFieldError returns common error for field #n repeated more than
// once.
func NewRepeatedFieldError(n protowire.Number) error {
	return fmt.Errorf("repeated field #%d", n)
}

// NewUnsupportedFieldError returns common error for unsupported field #n of
// type t.
func NewUnsupportedFieldError(n protowire.Number, t protowire.Type) error {
	return fmt.Errorf("unsupported field #%d of type %v", n, t)
}

// WrapParseFieldError wraps cause of parsing field #n of type t.
func WrapParseFieldError(n protowire.Number, t protowire.Type, cause error) error {
	return fmt.Errorf("parse field (#%d,type=%v): %w", n, t, cause)
}

// WrapSeekFieldError wraps cause of seeking field #n.
func WrapSeekFieldError(n protowire.Number, cause error) error {
	return fmt.Errorf("seek field %d: %w", n, cause)
}
