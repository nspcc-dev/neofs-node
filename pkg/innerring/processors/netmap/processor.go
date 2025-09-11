package netmap

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// EpochTimerReseter is a callback interface for tickers component.
	EpochTimerReseter interface {
		ResetEpochTimer(uint32) error
	}

	// EpochState is a callback interface for inner ring global state.
	EpochState interface {
		SetEpochCounter(uint64)
		EpochCounter() uint64
		SetEpochDuration(uint64)
		EpochDuration() time.Duration
	}

	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// NodeValidator wraps basic method of checking the correctness
	// of information about the node and its finalization for adding
	// to the network map.
	NodeValidator interface {
		// Verify must verify NodeInfo structure.
		//
		// Must return an error if NodeInfo input is invalid.
		Verify(netmap.NodeInfo) error
	}

	// Processor of events produced by network map contract
	// and new epoch ticker, because it is related to contract.
	Processor struct {
		log           *zap.Logger
		pool          *ants.Pool
		epochTimer    EpochTimerReseter
		epochState    EpochState
		alphabetState AlphabetState
		curMap        atomic.Value

		netmapClient *nmClient.Client
		containerWrp *container.Client

		handleAlphabetSync  event.Handler
		handleNotaryDeposit event.Handler

		nodeValidator NodeValidator

		forceContainersListUpdate *atomic.Bool
	}

	// Params of the processor constructor.
	Params struct {
		Log              *zap.Logger
		PoolSize         int
		NetmapClient     *nmClient.Client
		EpochTimer       EpochTimerReseter
		EpochState       EpochState
		AlphabetState    AlphabetState
		ContainerWrapper *container.Client

		AlphabetSyncHandler  event.Handler
		NotaryDepositHandler event.Handler

		NodeValidator NodeValidator
	}
)

const newEpochNotification = "NewEpoch"

// New creates network map contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/netmap: logger is not set")
	case p.EpochTimer == nil:
		return nil, errors.New("ir/netmap: epoch itmer is not set")
	case p.EpochState == nil:
		return nil, errors.New("ir/netmap: global state is not set")
	case p.AlphabetState == nil:
		return nil, errors.New("ir/netmap: global state is not set")
	case p.AlphabetSyncHandler == nil:
		return nil, errors.New("ir/netmap: alphabet sync handler is not set")
	case p.NotaryDepositHandler == nil:
		return nil, errors.New("ir/netmap: notary deposit handler is not set")
	case p.ContainerWrapper == nil:
		return nil, errors.New("ir/netmap: container contract wrapper is not set")
	case p.NodeValidator == nil:
		return nil, errors.New("ir/netmap: node validator is not set")
	}

	p.Log.Debug("netmap worker pool", zap.Int("size", p.PoolSize))

	curMap, err := p.NetmapClient.NetMap()
	if err != nil {
		return nil, fmt.Errorf("ir/netmap: can't fetch network map: %w", err)
	}

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/netmap: can't create worker pool: %w", err)
	}

	var (
		forceContainersListUpdate atomic.Bool
		migrationLog              = p.Log.With(zap.String("step", "0.49.0 Container contract migration"))
	)
	{ // 0.49.0 Container contract members fix
		var (
			cnrToCheck cid.ID
			nm         *netmap.NetMap
		)
		ids, err := p.ContainerWrapper.List(nil)
		if err != nil {
			return nil, fmt.Errorf("0.49.0 Container contract migration: cannot list containers: %w", err)
		}
		if len(ids) != 0 {
			nm, err = p.NetmapClient.NetMap()
			if err != nil {
				return nil, fmt.Errorf("0.49.0 Container contract migration: cannot fetch current network map: %w", err)
			}
		}
	cnrsLoop:
		for _, id := range ids {
			cnr, err := p.ContainerWrapper.Get(id[:])
			if err != nil {
				migrationLog.Warn("cannot fetch container", zap.Stringer("cID", id), zap.Error(err))
				continue
			}

			policy := cnr.PlacementPolicy()
			vectors, err := nm.ContainerNodes(policy, id)
			if err != nil {
				migrationLog.Warn("cannot build node placement policy", zap.Stringer("cID", id), zap.Error(err))
				continue
			}

			for _, vector := range vectors {
				if len(vector) != 0 {
					cnrToCheck = id
					break cnrsLoop
				}
			}
		}

		if !cnrToCheck.IsZero() {
			vectors, err := p.ContainerWrapper.Nodes(cnrToCheck)
			if err != nil {
				return nil, fmt.Errorf("0.49.0 Container contract migration: cannot fetch nodes for container %s: %w", cnrToCheck, err)
			}

			forceContainersListUpdate.Store(true)
			for _, vector := range vectors {
				if len(vector) != 0 {
					forceContainersListUpdate.Store(false)
					break
				}
			}
		}
	}

	if forceContainersListUpdate.Load() {
		p.Log.Info("0.49.0 Container contract migration: at new epoch start there will be forced Container contract list update")
	} else {
		p.Log.Info("0.49.0 Container contract migration: no need to migrate container lists")
	}

	var processor = &Processor{
		log:           p.Log,
		pool:          pool,
		epochTimer:    p.EpochTimer,
		epochState:    p.EpochState,
		alphabetState: p.AlphabetState,
		netmapClient:  p.NetmapClient,
		containerWrp:  p.ContainerWrapper,

		handleAlphabetSync: p.AlphabetSyncHandler,

		handleNotaryDeposit: p.NotaryDepositHandler,

		nodeValidator: p.NodeValidator,

		forceContainersListUpdate: &forceContainersListUpdate,
	}
	processor.curMap.Store(curMap)
	return processor, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	parsers := make([]event.NotificationParserInfo, 0, 1)

	var p event.NotificationParserInfo
	p.SetScriptHash(np.netmapClient.ContractAddress())

	// new epoch event
	p.SetType(newEpochNotification)
	p.SetParser(netmapEvent.ParseNewEpoch)
	parsers = append(parsers, p)

	return parsers
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	handlers := make([]event.NotificationHandlerInfo, 0, 1)

	var i event.NotificationHandlerInfo
	i.SetScriptHash(np.netmapClient.ContractAddress())

	// new epoch handler
	i.SetType(newEpochNotification)
	i.SetHandler(np.handleNewEpoch)
	handlers = append(handlers, i)

	return handlers
}

// ListenerNotaryParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	var (
		p event.NotaryParserInfo

		pp = make([]event.NotaryParserInfo, 0, 3)
	)

	p.SetScriptHash(np.netmapClient.ContractAddress())

	// new node
	p.SetRequestType(netmapEvent.AddNodeNotaryEvent)
	p.SetParser(netmapEvent.ParseAddNodeNotary)
	pp = append(pp, p)

	// update state
	p.SetRequestType(netmapEvent.UpdateStateNotaryEvent)
	p.SetParser(netmapEvent.ParseUpdatePeerNotary)
	pp = append(pp, p)

	return pp
}

// ListenerNotaryHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotaryHandlers() []event.NotaryHandlerInfo {
	var (
		h event.NotaryHandlerInfo

		hh = make([]event.NotaryHandlerInfo, 0, 3)
	)

	h.SetScriptHash(np.netmapClient.ContractAddress())

	// new node
	h.SetRequestType(netmapEvent.AddNodeNotaryEvent)
	h.SetHandler(np.handleAddNode)
	hh = append(hh, h)

	// update state
	h.SetRequestType(netmapEvent.UpdateStateNotaryEvent)
	h.SetHandler(np.handleUpdateState)
	hh = append(hh, h)

	return hh
}

// TimersHandlers for the 'Timers' event producer.
func (np *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}
