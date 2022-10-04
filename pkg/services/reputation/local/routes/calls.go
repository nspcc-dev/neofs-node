package routes

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// NextStage builds Manager list for trusting node and returns it directly.
//
// If passed route has more than one point, then endpoint of the route is reached.
func (b *Builder) NextStage(epoch uint64, t reputation.Trust, passed []common.ServerInfo) ([]common.ServerInfo, error) {
	passedLen := len(passed)

	b.log.Debug("building next stage for local trust route",
		logger.FieldUint("epoch", epoch),
		logger.FieldInt("passed_length", int64(passedLen)),
	)

	if passedLen > 1 {
		return nil, nil
	}

	route, err := b.managerBuilder.BuildManagers(epoch, t.TrustingPeer())
	if err != nil {
		return nil, fmt.Errorf("could not build managers for epoch: %d: %w", epoch, err)
	}

	return route, nil
}
