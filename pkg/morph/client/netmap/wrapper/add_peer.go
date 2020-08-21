package wrapper

// NodeInfo groups information about NeoFS storage node.
type NodeInfo struct{}

// AddPeer registers peer in NeoFS network through
// Netmap contract call.
func (w *Wrapper) AddPeer(nodeInfo NodeInfo) error {
	panic("implement me")
}
