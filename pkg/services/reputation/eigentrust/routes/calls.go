package routes

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
)

// NextStage builds Manager list for trusted node and returns it directly.
//
// If passed route has more than one point, then endpoint of the route is reached.
func (b *Builder) NextStage(epoch uint64, t reputation.Trust, passed []common.ServerInfo) ([]common.ServerInfo, error) {
	if len(passed) > 1 {
		return nil, nil
	}

	route, err := b.managerBuilder.BuildManagers(epoch, t.Peer())
	if err != nil {
		return nil, fmt.Errorf("could not build managers for epoch: %d: %w", epoch, err)
	}

	return route, nil
}
