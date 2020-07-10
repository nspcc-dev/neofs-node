package ir

// Info is a structure that groups the information
// about inner ring.
type Info struct {
	nodes []Node
}

// SetNodes is an IR node list setter.
func (s *Info) SetNodes(v []Node) {
	s.nodes = v
}

// Nodes is an IR node list getter.
func (s Info) Nodes() []Node {
	return s.nodes
}
