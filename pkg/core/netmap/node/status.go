package node

// Status represents a node
// status bits.
type Status uint64

const leftBitPos = 64

const (
	bitFullStorage = 1
)

// returns true if n-th left bit is set (starting at 0).
func isLeftBitSet(value Status, n uint8) bool {
	bitMask := Status(1 << (leftBitPos - n))
	return bitMask != 0 && value&bitMask == bitMask
}

// sets n-th left bit (starting at 0).
func setLeftBit(value *Status, n uint8) {
	*value |= Status(1 << (leftBitPos - n))
}

// resets n-th left bit (starting at 0).
func resetLeftBit(value *Status, n uint8) {
	*value &= ^Status(1 << (leftBitPos - n))
}

// Full returns true if node is in Full status.
//
// Full status marks node has enough space
// for storing users objects.
func (n Status) Full() bool {
	return isLeftBitSet(n, bitFullStorage)
}

// SetFull sets Full status of node.
func (n *Status) SetFull() {
	setLeftBit(n, bitFullStorage)
}

// ResetFull resets Full status of node.
func (n *Status) ResetFull() {
	resetLeftBit(n, bitFullStorage)
}

// StatusFromUint64 converts builtin
// uint64 value to Status.
//
// Try to avoid direct cast for
// better portability.
func StatusFromUint64(v uint64) Status {
	return Status(v)
}

// StatusToUint64 converts Status value
// to builtin uint64.
//
// Try to avoid direct cast for
// better portability.
func StatusToUint64(s Status) uint64 {
	return uint64(s)
}
