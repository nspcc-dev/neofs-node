package node

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"go.uber.org/dig"
)

type (
	placementToolParams struct {
		dig.In

		Placement placement.Component
	}

	placementToolResult struct {
		dig.Out

		Placer *placement.PlacementWrapper
	}
)

func newPlacementTool(p placementToolParams) (res placementToolResult, err error) {
	if res.Placer, err = placement.NewObjectPlacer(p.Placement); err != nil {
		return
	}

	return
}
