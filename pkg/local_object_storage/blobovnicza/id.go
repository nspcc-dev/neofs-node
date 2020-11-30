package blobovnicza

// ID represents Blobovnicza identifier.
type ID []byte

// NewIDFromBytes constructs ID from byte slice.
func NewIDFromBytes(v []byte) *ID {
	return (*ID)(&v)
}

func (id ID) String() string {
	return string(id)
}
