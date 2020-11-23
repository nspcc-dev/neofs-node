package util

import (
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// LogServiceError writes debug error message of object service to provided logger.
func LogServiceError(l *logger.Logger, req string, node *network.Address, err error) {
	l.Debug("object service error",
		zap.Stringer("node", node),
		zap.String("request", req),
		zap.String("error", err.Error()),
	)
}
