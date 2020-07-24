package network

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/morph"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapevent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network/peers"
	state "github.com/nspcc-dev/neofs-node/pkg/network/transport/state/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	placementParams struct {
		dig.In

		Log     *zap.Logger
		Peers   peers.Store
		Fetcher storage.Storage

		MorphEventListener event.Listener

		NetMapClient *contract.Wrapper

		MorphEventHandlers morph.EventHandlers
	}

	placementOutput struct {
		dig.Out

		Placement placement.Component
		Healthy   state.HealthChecker `group:"healthy"`
	}
)

const defaultChronologyDuraion = 2

func newPlacement(p placementParams) placementOutput {
	place := placement.New(placement.Params{
		Log:                p.Log,
		Peerstore:          p.Peers,
		Fetcher:            p.Fetcher,
		ChronologyDuration: defaultChronologyDuraion,
	})

	if handlerInfo, ok := p.MorphEventHandlers[morph.ContractEventOptPath(
		morph.NetmapContractName,
		morph.NewEpochEventType,
	)]; ok {
		handlerInfo.SetHandler(func(ev event.Event) {
			nm, err := p.NetMapClient.GetNetMap()
			if err != nil {
				p.Log.Error("could not get network map",
					zap.String("error", err.Error()),
				)
				return
			}

			if err := place.Update(
				ev.(netmapevent.NewEpoch).EpochNumber(),
				nm,
			); err != nil {
				p.Log.Error("could not update network map in placement component",
					zap.String("error", err.Error()),
				)
			}
		})

		p.MorphEventListener.RegisterHandler(handlerInfo)
	}

	return placementOutput{
		Placement: place,
		Healthy:   place.(state.HealthChecker),
	}
}
