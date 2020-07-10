package ir

// Node is a structure that groups
// the information about IR node.
type Node struct {
	key []byte
}

// SetKey is an IR node public key setter.
func (s *Node) SetKey(v []byte) {
	s.key = v
}

// Key is an IR node public key getter.
func (s Node) Key() []byte {
	return s.key
}
