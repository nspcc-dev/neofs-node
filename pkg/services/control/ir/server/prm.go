package control

import (
	"crypto/ecdsa"
)

// Prm groups required parameters of
// Server's constructor.
type Prm struct {
	key *ecdsa.PrivateKey

	healthChecker HealthChecker
}

// SetPrivateKey sets private key to sign responses.
func (x *Prm) SetPrivateKey(key *ecdsa.PrivateKey) {
	x.key = key
}

// SetHealthChecker sets HealthChecker to calculate
// health status.
func (x *Prm) SetHealthChecker(hc HealthChecker) {
	x.healthChecker = hc
}
