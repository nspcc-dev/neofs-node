package logicerr

import (
	"errors"
	"fmt"
)

// ErrStreamAborted is returned when some stream is already aborted.
var ErrStreamAborted = errors.New("stream already aborted")

// Error is wrapped to highlight the business logic errors.
var Error = errors.New("logical error")

// New returns simple error with a provided error message.
func New(msg string) error {
	return Wrap(errors.New(msg))
}

// Wrap wraps arbitrary error into a logical one.
func Wrap(err error) error {
	return fmt.Errorf("%w: %w", Error, err)
}
