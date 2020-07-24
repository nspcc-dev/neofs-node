package epoch

// Epoch represents the
// number of NeoFS epoch.
type Epoch uint64

// FromUint64 converts builtin
// uint64 value to Epoch.
//
// Try to avoid direct cast for
// better portability.
func FromUint64(e uint64) Epoch {
	return Epoch(e)
}

// ToUint64 converts Epoch value
// to builtin uint64.
//
// Try to avoid direct cast for
// better portability.
func ToUint64(e Epoch) uint64 {
	return uint64(e)
}
