package control

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
)

// HealthChecker is component interface for calculating
// the current health status of a node.
type HealthChecker interface {
	// HealthStatus must calculate and return current health status of the IR application.
	//
	// If status can not be calculated for any reason,
	// control.HealthStatus_HEALTH_STATUS_UNDEFINED should be returned.
	HealthStatus() control.HealthStatus
}

// NotaryManager is component interface for working with the network.
type NotaryManager interface {
	// ListNotaryRequests must return a list of hashes from notary pool.
	ListNotaryRequests() ([]util.Uint256, error)

	// RequestNotary must send a notary request with the given method and arguments.
	RequestNotary(method string, args ...[]byte) (util.Uint256, error)

	// SignNotary must sign an existing notary transaction with the given hash.
	SignNotary(hash util.Uint256) error
}
