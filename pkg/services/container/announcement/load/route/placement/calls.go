package placementrouter

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	loadroute "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/route"
	"github.com/pkg/errors"
)

// NextStage composes container nodes for the container and epoch from a,
// and returns the list of nodes with maximum weight (one from each vector).
//
// If passed route has more than one point, then endpoint of the route is reached.
//
// The traversed route is not checked, it is assumed to be correct.
func (b *Builder) NextStage(a container.UsedSpaceAnnouncement, passed []loadroute.ServerInfo) ([]loadroute.ServerInfo, error) {
	if len(passed) > 1 {
		return nil, nil
	}

	placement, err := b.placementBuilder.BuildPlacement(a.Epoch(), a.ContainerID())
	if err != nil {
		return nil, errors.Wrapf(err, "could not build placement %s", a.ContainerID())
	}

	res := make([]loadroute.ServerInfo, 0, len(placement))

	for i := range placement {
		if len(placement[i]) == 0 {
			continue
		}

		res = append(res, placement[i][0])
	}

	return res, nil
}
