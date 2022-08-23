package common

import "errors"

// ErrReadOnly MUST be returned for modifying operations when the storage was opened
// in readonly mode.
var ErrReadOnly = errors.New("opened as read-only")
