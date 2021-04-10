package reputation

import (
	"strconv"
)

// TrustValue represents the numeric value of the node's trust.
type TrustValue float64

const (
	// TrustOne is a trust value equal to one.
	TrustOne = TrustValue(1)

	// TrustZero is a trust value equal to zero.
	TrustZero = TrustValue(0)
)

// TrustValueFromFloat64 converts float64 to TrustValue.
func TrustValueFromFloat64(v float64) TrustValue {
	return TrustValue(v)
}

// TrustValueFromInt converts int to TrustValue.
func TrustValueFromInt(v int) TrustValue {
	return TrustValue(v)
}

func (v TrustValue) String() string {
	return strconv.FormatFloat(float64(v), 'f', -1, 64)
}

// Float64 converts TrustValue to float64.
func (v TrustValue) Float64() float64 {
	return float64(v)
}

// Add adds v2 to v.
func (v *TrustValue) Add(v2 TrustValue) {
	*v = *v + v2
}

// Div returns the result of dividing v by v2.
func (v TrustValue) Div(v2 TrustValue) TrustValue {
	return v / v2
}

// Mul multiplies v by v2.
func (v *TrustValue) Mul(v2 TrustValue) {
	*v *= v2
}

// IsZero returns true if v equal to zero.
func (v TrustValue) IsZero() bool {
	return v == 0
}

// Trust represents peer's trust (reputation).
type Trust struct {
	trusting, peer PeerID

	val TrustValue
}

// TrustHandler describes the signature of the reputation.Trust
// value handling function.
//
// Termination of processing without failures is usually signaled
// with a zero error, while a specific value may describe the reason
// for failure.
type TrustHandler func(Trust) error

// Value returns peer's trust value.
func (t Trust) Value() TrustValue {
	return t.val
}

// SetValue sets peer's trust value.
func (t *Trust) SetValue(val TrustValue) {
	t.val = val
}

// Peer returns trusted peer ID.
func (t Trust) Peer() PeerID {
	return t.peer
}

// SetPeer sets trusted peer ID.
func (t *Trust) SetPeer(id PeerID) {
	t.peer = id
}

// TrustingPeer returns trusting peer ID.
func (t Trust) TrustingPeer() PeerID {
	return t.trusting
}

// SetTrustingPeer sets trusting peer ID.
func (t *Trust) SetTrustingPeer(id PeerID) {
	t.trusting = id
}
