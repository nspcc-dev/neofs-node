package protoscan

import "fmt"

// newParseFieldError returns common error for failed f's parsing.
func newParseFieldError(f MessageField, cause error) error {
	return fmt.Errorf("parse %s field: %w", f, cause)
}

// newWrongLenError returns common error for wrong field length.
func newWrongLenError(got, exp int) error {
	return fmt.Errorf("wrong len %d bytes instead of %d", got, exp)
}
