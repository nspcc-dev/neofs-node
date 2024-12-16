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

type NotaryManager interface {
	ListNotaryRequests() ([]string, error)

	RequestNotary([]byte) (string, error)

	SignNotary(util.Uint256) (string, error)
}
