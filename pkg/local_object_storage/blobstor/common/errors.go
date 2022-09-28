package common

import "errors"

// ErrReadOnly MUST be returned for modifying operations when the storage was opened
// in readonly mode.
var ErrReadOnly = errors.New("opened as read-only")

// ErrNoSpace MUST be returned when there is no space to put an object on the device.
var ErrNoSpace = errors.New("no free space")
