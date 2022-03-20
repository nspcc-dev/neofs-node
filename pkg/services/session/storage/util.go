package storage

import (
	"fmt"

	"github.com/google/uuid"
)

// NewTokenID generates new ID for a token
// based on UUID.
func NewTokenID() ([]byte, error) {
	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("could not generate UUID: %w", err)
	}

	uidBytes, err := uid.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("could not marshal marshal UUID: %w", err)
	}

	return uidBytes, nil
}
