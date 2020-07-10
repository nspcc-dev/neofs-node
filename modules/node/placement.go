package node

import (
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/placement"
	"go.uber.org/dig"
)

type (
	placementToolParams struct {
		dig.In

		Placement placement.Component
	}

	placementToolResult struct {
		dig.Out

		Placer implementations.ObjectPlacer

		Receiver implementations.EpochReceiver
	}
)

func newPlacementTool(p placementToolParams) (res placementToolResult, err error) {
	if res.Placer, err = implementations.NewObjectPlacer(p.Placement); err != nil {
		return
	}

	res.Receiver = res.Placer

	return
}
