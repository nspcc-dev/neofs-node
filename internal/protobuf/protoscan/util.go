package protoscan

import "fmt"

// newParseFieldError returns common error for failed f's parsing.
func newParseFieldError(f MessageField, cause error) error {
	return fmt.Errorf("parse %s field: %w", f, cause)
}
