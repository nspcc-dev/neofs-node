package logicerr

import "errors"

// Logical is a wrapper for logical errors.
type Logical struct {
	err error
}

// New returns simple error with a provided error message.
func New(msg string) Logical {
	return Wrap(errors.New(msg))
}

// Error implements the error interface.
func (e Logical) Error() string {
	return e.err.Error()
}

// Wrap wraps arbitrary error into a logical one.
func Wrap(err error) Logical {
	return Logical{err: err}
}

// Unwrap returns underlying error.
func (e Logical) Unwrap() error {
	return e.err
}
