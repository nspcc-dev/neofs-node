package util

import (
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// LogServiceError writes debug error message of object service to provided logger.
func LogServiceError(l *logger.Logger, req string, node network.AddressGroup, err error) {
	l.Debug("object service error",
		logger.FieldString("node", network.StringifyGroup(node)),
		logger.FieldString("request", req),
		logger.FieldError(err),
	)
}

// LogWorkerPoolError writes debug error message of object worker pool to provided logger.
func LogWorkerPoolError(l *logger.Logger, req string, err error) {
	l.Debug("could not push task to worker pool",
		logger.FieldString("request", req),
		logger.FieldError(err),
	)
}
