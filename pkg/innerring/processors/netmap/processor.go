package netmap

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
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
		SetEpochDuration(uint64)
		EpochDuration() uint64
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
		log           *zap.Logger
		pool          *ants.Pool
		epochTimer    EpochTimerReseter
		epochState    EpochState
		alphabetState AlphabetState

		netmapClient *nmWrapper.Wrapper
		containerWrp *container.Wrapper

		netmapSnapshot cleanupTable

		handleNewAudit         event.Handler
		handleAuditSettlements event.Handler
		handleAlphabetSync     event.Handler
		handleNotaryDeposit    event.Handler

		nodeValidator NodeValidator

		notaryDisabled bool
	}

	// Params of the processor constructor.
	Params struct {
		Log              *zap.Logger
		PoolSize         int
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
		NotaryDepositHandler    event.Handler

		NodeValidator NodeValidator

		NotaryDisabled bool
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
	case p.NotaryDepositHandler == nil:
		return nil, errors.New("ir/netmap: notary deposit handler is not set")
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
		epochTimer:     p.EpochTimer,
		epochState:     p.EpochState,
		alphabetState:  p.AlphabetState,
		netmapClient:   p.NetmapClient,
		containerWrp:   p.ContainerWrapper,
		netmapSnapshot: newCleanupTable(p.CleanupEnabled, p.CleanupThreshold),
		handleNewAudit: p.HandleAudit,

		handleAuditSettlements: p.AuditSettlementsHandler,

		handleAlphabetSync: p.AlphabetSyncHandler,

		handleNotaryDeposit: p.NotaryDepositHandler,

		nodeValidator: p.NodeValidator,

		notaryDisabled: p.NotaryDisabled,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	parsers := make([]event.NotificationParserInfo, 0, 3)

	var p event.NotificationParserInfo

	p.SetScriptHash(np.netmapClient.ContractAddress())

	// new epoch event
	p.SetType(newEpochNotification)
	p.SetParser(netmapEvent.ParseNewEpoch)
	parsers = append(parsers, p)

	if !np.notaryDisabled {
		return parsers
	}

	// new peer event
	p.SetType(addPeerNotification)
	p.SetParser(netmapEvent.ParseAddPeer)
	parsers = append(parsers, p)

	// update peer event
	p.SetType(updatePeerStateNotification)
	p.SetParser(netmapEvent.ParseUpdatePeer)
	parsers = append(parsers, p)

	return parsers
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	handlers := make([]event.NotificationHandlerInfo, 0, 3)

	var i event.NotificationHandlerInfo

	i.SetScriptHash(np.netmapClient.ContractAddress())

	// new epoch handler
	i.SetType(newEpochNotification)
	i.SetHandler(np.handleNewEpoch)
	handlers = append(handlers, i)

	if !np.notaryDisabled {
		return handlers
	}

	// new peer handler
	i.SetType(addPeerNotification)
	i.SetHandler(np.handleAddPeer)
	handlers = append(handlers, i)

	// update peer handler
	i.SetType(updatePeerStateNotification)
	i.SetHandler(np.handleUpdateState)
	handlers = append(handlers, i)

	return handlers
}

// ListenerNotaryParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	var (
		p event.NotaryParserInfo

		pp = make([]event.NotaryParserInfo, 0, 2)
	)

	p.SetMempoolType(mempoolevent.TransactionAdded)
	p.SetScriptHash(np.netmapClient.ContractAddress())

	// new peer
	p.SetRequestType(netmapEvent.AddPeerNotaryEvent)
	p.SetParser(netmapEvent.ParseAddPeerNotary)
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

		hh = make([]event.NotaryHandlerInfo, 0, 2)
	)

	h.SetMempoolType(mempoolevent.TransactionAdded)
	h.SetScriptHash(np.netmapClient.ContractAddress())

	// new peer
	h.SetRequestType(netmapEvent.AddPeerNotaryEvent)
	h.SetHandler(np.handleAddPeer)
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
