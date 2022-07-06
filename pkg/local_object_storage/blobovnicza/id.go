package blobovnicza

// ID represents Blobovnicza identifier.
type ID []byte

// NewIDFromBytes constructs an ID from a byte slice.
func NewIDFromBytes(v []byte) *ID {
	return (*ID)(&v)
}

func (id ID) String() string {
	return string(id)
}

func (id ID) Bytes() []byte {
	return id
}
