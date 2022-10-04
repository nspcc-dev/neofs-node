package util

// NodeState is storage node state processed by Object service.
type NodeState interface {
	// IsMaintenance checks if node is under maintenance. Node MUST NOT serve
	// local object operations. Node MUST respond with apistatus.NodeUnderMaintenance
	// error if IsMaintenance returns true.
	IsMaintenance() bool
}
