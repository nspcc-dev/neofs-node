package errors

import (
	"errors"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectID is an object ID as error.
type ObjectID oid.ID

// Error returns string-encoded object ID.
func (x ObjectID) Error() string {
	return oid.ID(x).String()
}

// ParentObjectError is an error indicating some object as a parent for some reason.
type ParentObjectError struct{ error }

// ErrParentObject allows to check whether error is [ParentObjectError] via [errors.Is].
var ErrParentObject ParentObjectError

// NewParentObjectError constructs ParentObjectError from given cause.
func NewParentObjectError(cause error) ParentObjectError {
	return ParentObjectError{error: cause}
}

// Unwrap implements interface for [errors] package funcs.
func (x ParentObjectError) Unwrap() error { return x.error }

// Is implements interface for [errors.Is].
func (x ParentObjectError) Is(target error) bool {
	if _, ok := target.(ParentObjectError); ok {
		return true
	}
	return errors.Is(x.error, target)
}
