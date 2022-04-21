package netmap

// AnnouncedKeys is an interface of utility for working with the announced public keys of the storage nodes.
type AnnouncedKeys interface {
	// Checks if the key was announced by a local node.
	IsLocalKey(key []byte) bool
}
