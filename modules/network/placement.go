package network

import (
	"github.com/nspcc-dev/neofs-node/lib/blockchain/event"
	netmapevent "github.com/nspcc-dev/neofs-node/lib/blockchain/event/netmap"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/nspcc-dev/neofs-node/lib/placement"
	"github.com/nspcc-dev/neofs-node/modules/morph"
	"github.com/nspcc-dev/neofs-node/services/public/state"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	placementParams struct {
		dig.In

		Log     *zap.Logger
		Peers   peers.Store
		Fetcher libcnr.Storage

		MorphEventListener event.Listener

		NetMapStorage netmap.Storage

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
			nmRes, err := p.NetMapStorage.GetNetMap(netmap.GetParams{})
			if err != nil {
				p.Log.Error("could not get network map",
					zap.String("error", err.Error()),
				)
				return
			}

			if err := place.Update(
				ev.(netmapevent.NewEpoch).EpochNumber(),
				nmRes.NetMap(),
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
