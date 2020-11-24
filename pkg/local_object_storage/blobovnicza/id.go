package blobovnicza

import (
	"github.com/google/uuid"
)

// ID represents Blobovnicza identifier.
type ID uuid.UUID

// NewIDFromUUID constructs ID from UUID instance.
func NewIDFromUUID(uid uuid.UUID) *ID {
	return (*ID)(&uid)
}

func (id ID) String() string {
	return (uuid.UUID)(id).String()
}
