package control

import control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"

// HealthChecker is component interface for calculating
// the current health status of a node.
type HealthChecker interface {
	// Must calculate and return current health status of the IR application.
	//
	// If status can not be calculated for any reason,
	// control.HealthStatus_HEALTH_STATUS_UNDEFINED should be returned.
	HealthStatus() control.HealthStatus
}
