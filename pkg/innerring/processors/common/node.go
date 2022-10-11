package common

// NodeStatus represents status of the local Inner Ring node in the NeoFS network.
type NodeStatus interface {
	// IsAlphabet checks if local node is registered in the NeoFS network
	// as an Alphabet node. Returns any error encountered which prevented
	// the status to be determined.
	IsAlphabet() (bool, error)
}
