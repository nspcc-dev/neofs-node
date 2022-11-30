package common

import "fmt"

// BboltFatalHandler catches bbolt database panic and wraps the error in `ErrFatal`.
// It is intended to be executed in `defer` of all bbolt-related routines.
func BboltFatalHandler(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("%w: %v", ErrFatal, r)
	}
}
