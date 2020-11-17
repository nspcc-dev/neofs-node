package shard

// ID represents Shard identifier.
//
// Each shard should have the unique ID within
// a single instance of local storage.
type ID []byte

// NewIDFromBytes constructs ID from byte slice.
func NewIDFromBytes(v []byte) *ID {
	return (*ID)(&v)
}

func (id ID) String() string {
	return string(id)
}

// ID returns Shard identifier.
func (s *Shard) ID() *ID {
	return s.id
}
