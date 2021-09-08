package netmap

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	container "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	nmWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// EpochTimerReseter is a callback interface for tickers component.
	EpochTimerReseter interface {
		ResetEpochTimer() error
	}

	// EpochState is a callback interface for inner ring global state.
	EpochState interface {
		SetEpochCounter(uint64)
		EpochCounter() uint64
	}

	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// NodeValidator wraps basic method of checking the correctness
	// of information about the node and its finalization for adding
	// to the network map.
	NodeValidator interface {
		// Must verify and optionally update NodeInfo structure.
		//
		// Must return an error if NodeInfo input is invalid.
		// Must return an error if it is not possible to correctly
		// change the structure for sending to the network map.
		//
		// If no error occurs, the parameter must point to the
		// ready-made NodeInfo structure.
		VerifyAndUpdate(*netmap.NodeInfo) error
	}

	// Processor of events produced by network map contract
	// and new epoch ticker, because it is related to contract.
	Processor struct {
		log            *zap.Logger
		pool           *ants.Pool
		netmapContract util.Uint160
		epochTimer     EpochTimerReseter
		epochState     EpochState
		alphabetState  AlphabetState

		netmapClient *nmWrapper.Wrapper
		containerWrp *container.Wrapper

		netmapSnapshot cleanupTable

		handleNewAudit         event.Handler
		handleAuditSettlements event.Handler
		handleAlphabetSync     event.Handler

		nodeValidator NodeValidator
	}

	// Params of the processor constructor.
	Params struct {
		Log      *zap.Logger
		PoolSize int
		// TODO(@fyrchik): add `ContractHash` method to the NetmapClient and remove this parameter.
		NetmapContract   util.Uint160
		NetmapClient     *nmWrapper.Wrapper
		EpochTimer       EpochTimerReseter
		EpochState       EpochState
		AlphabetState    AlphabetState
		CleanupEnabled   bool
		CleanupThreshold uint64 // in epochs
		ContainerWrapper *container.Wrapper

		HandleAudit             event.Handler
		AuditSettlementsHandler event.Handler
		AlphabetSyncHandler     event.Handler

		NodeValidator NodeValidator
	}
)

const (
	newEpochNotification        = "NewEpoch"
	addPeerNotification         = "AddPeer"
	updatePeerStateNotification = "UpdateState"
)

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
	case p.HandleAudit == nil:
		return nil, errors.New("ir/netmap: audit handler is not set")
	case p.AuditSettlementsHandler == nil:
		return nil, errors.New("ir/netmap: audit settlement handler is not set")
	case p.AlphabetSyncHandler == nil:
		return nil, errors.New("ir/netmap: alphabet sync handler is not set")
	case p.ContainerWrapper == nil:
		return nil, errors.New("ir/netmap: container contract wrapper is not set")
	case p.NodeValidator == nil:
		return nil, errors.New("ir/netmap: node validator is not set")
	}

	p.Log.Debug("netmap worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/netmap: can't create worker pool: %w", err)
	}

	return &Processor{
		log:            p.Log,
		pool:           pool,
		netmapContract: p.NetmapContract,
		epochTimer:     p.EpochTimer,
		epochState:     p.EpochState,
		alphabetState:  p.AlphabetState,
		netmapClient:   p.NetmapClient,
		containerWrp:   p.ContainerWrapper,
		netmapSnapshot: newCleanupTable(p.CleanupEnabled, p.CleanupThreshold),
		handleNewAudit: p.HandleAudit,

		handleAuditSettlements: p.AuditSettlementsHandler,

		handleAlphabetSync: p.AlphabetSyncHandler,

		nodeValidator: p.NodeValidator,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	var parsers []event.NotificationParserInfo

	// new epoch event
	newEpoch := event.NotificationParserInfo{}
	newEpoch.SetType(newEpochNotification)
	newEpoch.SetScriptHash(np.netmapContract)
	newEpoch.SetParser(netmapEvent.ParseNewEpoch)
	parsers = append(parsers, newEpoch)

	// new peer event
	addPeer := event.NotificationParserInfo{}
	addPeer.SetType(addPeerNotification)
	addPeer.SetScriptHash(np.netmapContract)
	addPeer.SetParser(netmapEvent.ParseAddPeer)
	parsers = append(parsers, addPeer)

	// update peer event
	updatePeer := event.NotificationParserInfo{}
	updatePeer.SetType(updatePeerStateNotification)
	updatePeer.SetScriptHash(np.netmapContract)
	updatePeer.SetParser(netmapEvent.ParseUpdatePeer)
	parsers = append(parsers, updatePeer)

	return parsers
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	var handlers []event.NotificationHandlerInfo

	// new epoch handler
	newEpoch := event.NotificationHandlerInfo{}
	newEpoch.SetType(newEpochNotification)
	newEpoch.SetScriptHash(np.netmapContract)
	newEpoch.SetHandler(np.handleNewEpoch)
	handlers = append(handlers, newEpoch)

	// new peer handler
	addPeer := event.NotificationHandlerInfo{}
	addPeer.SetType(addPeerNotification)
	addPeer.SetScriptHash(np.netmapContract)
	addPeer.SetHandler(np.handleAddPeer)
	handlers = append(handlers, addPeer)

	// update peer handler
	updatePeer := event.NotificationHandlerInfo{}
	updatePeer.SetType(updatePeerStateNotification)
	updatePeer.SetScriptHash(np.netmapContract)
	updatePeer.SetHandler(np.handleUpdateState)
	handlers = append(handlers, updatePeer)

	return handlers
}

// ListenerNotaryParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	return nil
}

// ListenerNotaryHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotaryHandlers() []event.NotaryHandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (np *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}
