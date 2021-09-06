package netmap

// AnnouncedKeys is an interface of utility for working with announced public keys of the storage nodes.
type AnnouncedKeys interface {
	// Checks if key was announced by local node.
	IsLocalKey(key []byte) bool
}
