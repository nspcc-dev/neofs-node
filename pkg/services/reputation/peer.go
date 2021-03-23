package reputation

const peerIDLength = 33

// PeerID represents identifier of reputation system participant.
type PeerID [peerIDLength]byte

// Bytes converts PeerID to []byte.
func (id PeerID) Bytes() []byte {
	return id[:]
}

// PeerIDFromBytes restores PeerID from []byte.
func PeerIDFromBytes(data []byte) (id PeerID) {
	copy(id[:], data)
	return
}
