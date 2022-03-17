package object

import "errors"

// ErrRangeOutOfBounds is a basic error of violation of the boundaries of the
// payload of an object.
var ErrRangeOutOfBounds = errors.New("payload range is out of bounds")
