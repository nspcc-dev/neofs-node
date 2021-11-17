package object

import "errors"

// ErrNotFound is a basic "not found" error returned by
// object read functions.
var ErrNotFound = errors.New("object not found")

// ErrRangeOutOfBounds is a basic error of violation of the boundaries of the
// payload of an object.
var ErrRangeOutOfBounds = errors.New("payload range is out of bounds")

// ErrAlreadyRemoved returned when object has tombstone in graveyard.
var ErrAlreadyRemoved = errors.New("object already removed")
