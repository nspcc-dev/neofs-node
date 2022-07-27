package object

import "errors"

// ErrObjectIsExpired is returned when the requested object's
// epoch is less than the current one. Such objects are considered
// as removed and should not be returned from the Storage Engine.
var ErrObjectIsExpired = errors.New("object is expired")
