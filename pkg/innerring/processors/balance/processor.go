package balance

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	balanceEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/balance"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// ActiveState is a callback interface for inner ring global state
	ActiveState interface {
		IsActive() bool
	}

	// Processor of events produced by balance contract in morph chain.
	Processor struct {
		log             *zap.Logger
		pool            *ants.Pool
		neofsContract   util.Uint160
		balanceContract util.Uint160
		mainnetClient   *client.Client
		activeState     ActiveState
	}

	// Params of the processor constructor.
	Params struct {
		Log             *zap.Logger
		PoolSize        int
		NeoFSContract   util.Uint160
		BalanceContract util.Uint160
		MainnetClient   *client.Client
		ActiveState     ActiveState
	}
)

const (
	lockNotification = "Lock"
)

// New creates balance contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/balance: logger is not set")
	case p.MainnetClient == nil:
		return nil, errors.New("ir/balance: neo:mainnet client is not set")
	case p.ActiveState == nil:
		return nil, errors.New("ir/balance: global state is not set")
	}

	p.Log.Debug("balance worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, errors.Wrap(err, "ir/balance: can't create worker pool")
	}

	return &Processor{
		log:             p.Log,
		pool:            pool,
		neofsContract:   p.NeoFSContract,
		balanceContract: p.BalanceContract,
		mainnetClient:   p.MainnetClient,
		activeState:     p.ActiveState,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (bp *Processor) ListenerParsers() []event.ParserInfo {
	var parsers []event.ParserInfo

	// new lock event
	lock := event.ParserInfo{}
	lock.SetType(lockNotification)
	lock.SetScriptHash(bp.balanceContract)
	lock.SetParser(balanceEvent.ParseLock)
	parsers = append(parsers, lock)

	return parsers
}

// ListenerHandlers for the 'event.Listener' event producer.
func (bp *Processor) ListenerHandlers() []event.HandlerInfo {
	var handlers []event.HandlerInfo

	// lock handler
	lock := event.HandlerInfo{}
	lock.SetType(lockNotification)
	lock.SetScriptHash(bp.balanceContract)
	lock.SetHandler(bp.handleLock)
	handlers = append(handlers, lock)

	return handlers
}

// TimersHandlers for the 'Timers' event producer.
func (bp *Processor) TimersHandlers() []event.HandlerInfo {
	return nil
}
