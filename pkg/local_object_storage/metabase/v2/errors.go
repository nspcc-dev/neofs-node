package meta

import (
	"errors"
)

var (
	// ErrNotFound returned when object header should exist in primary index but
	// it is not present there.
	ErrNotFound = errors.New("address not found")

	// ErrAlreadyRemoved returned when object has tombstone in graveyard.
	ErrAlreadyRemoved = errors.New("object already removed")
)
