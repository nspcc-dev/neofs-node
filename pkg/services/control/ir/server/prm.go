package control

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
)

// Prm groups required parameters of
// Server's constructor.
type Prm struct {
	key keys.PrivateKey

	healthChecker HealthChecker
}

// SetPrivateKey sets private key to sign responses.
func (x *Prm) SetPrivateKey(key keys.PrivateKey) {
	x.key = key
}

// SetHealthChecker sets HealthChecker to calculate
// health status.
func (x *Prm) SetHealthChecker(hc HealthChecker) {
	x.healthChecker = hc
}
