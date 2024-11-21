package util

import (
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"go.uber.org/zap"
)

// LogServiceError writes error message of object service to provided logger.
func LogServiceError(l *zap.Logger, req string, node network.AddressGroup, err error) {
	l.Error("object service error",
		zap.String("node", network.StringifyGroup(node)),
		zap.String("request", req),
		zap.Error(err),
	)
}

// LogWorkerPoolError writes debug error message of object worker pool to provided logger.
func LogWorkerPoolError(l *zap.Logger, req string, err error) {
	l.Error("could not push task to worker pool",
		zap.String("request", req),
		zap.Error(err),
	)
}
