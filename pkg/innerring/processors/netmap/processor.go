package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// EpochTimerReseter is a callback interface for tickers component.
	EpochTimerReseter interface {
		ResetEpochTimer()
	}

	// EpochState is a callback interface for inner ring global state.
	EpochState interface {
		SetEpochCounter(uint64)
		EpochCounter() uint64
	}

	// ActiveState is a callback interface for inner ring global state.
	ActiveState interface {
		IsActive() bool
	}

	// Processor of events produced by network map contract
	// and new epoch ticker, because it is related to contract.
	Processor struct {
		log            *zap.Logger
		pool           *ants.Pool
		netmapContract util.Uint160
		epochTimer     EpochTimerReseter
		epochState     EpochState
		activeState    ActiveState
		morphClient    *client.Client
	}

	// Params of the processor constructor.
	Params struct {
		Log            *zap.Logger
		PoolSize       int
		NetmapContract util.Uint160
		EpochTimer     EpochTimerReseter
		MorphClient    *client.Client
		EpochState     EpochState
		ActiveState    ActiveState
	}
)

const (
	newEpochNotification = "NewEpoch"
)

// New creates network map contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/netmap: logger is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/netmap: morph client is not set")
	case p.EpochTimer == nil:
		return nil, errors.New("ir/netmap: epoch itmer is not set")
	case p.EpochState == nil:
		return nil, errors.New("ir/netmap: global state is not set")
	case p.ActiveState == nil:
		return nil, errors.New("ir/netmap: global state is not set")
	}

	p.Log.Debug("netmap worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, errors.Wrap(err, "ir/netmap: can't create worker pool")
	}

	return &Processor{
		log:            p.Log,
		pool:           pool,
		netmapContract: p.NetmapContract,
		epochTimer:     p.EpochTimer,
		epochState:     p.EpochState,
		activeState:    p.ActiveState,
		morphClient:    p.MorphClient,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerParsers() []event.ParserInfo {
	var parsers []event.ParserInfo

	// new epoch event
	newEpoch := event.ParserInfo{}
	newEpoch.SetType(newEpochNotification)
	newEpoch.SetScriptHash(np.netmapContract)
	newEpoch.SetParser(netmapEvent.ParseNewEpoch)
	parsers = append(parsers, newEpoch)

	return parsers
}

// ListenerHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerHandlers() []event.HandlerInfo {
	var handlers []event.HandlerInfo

	// new epoch handler
	newEpoch := event.HandlerInfo{}
	newEpoch.SetType(newEpochNotification)
	newEpoch.SetScriptHash(np.netmapContract)
	newEpoch.SetHandler(np.handleNewEpoch)
	handlers = append(handlers, newEpoch)

	return handlers
}

// TimersHandlers for the 'Timers' event producer.
func (np *Processor) TimersHandlers() []event.HandlerInfo {
	var handlers []event.HandlerInfo

	// new epoch handler
	newEpoch := event.HandlerInfo{}
	newEpoch.SetType(timers.EpochTimer)
	newEpoch.SetHandler(np.handleNewEpochTick)
	handlers = append(handlers, newEpoch)

	return handlers
}
