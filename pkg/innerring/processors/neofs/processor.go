package neofs

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// EpochState is a callback interface for inner ring global state
	EpochState interface {
		EpochCounter() uint64
	}

	// ActiveState is a callback interface for inner ring global state
	ActiveState interface {
		IsActive() bool
	}

	// Processor of events produced by neofs contract in main net.
	Processor struct {
		log             *zap.Logger
		pool            *ants.Pool
		neofsContract   util.Uint160
		balanceContract util.Uint160
		morphClient     *client.Client
		epochState      EpochState
		activeState     ActiveState
	}

	// Params of the processor constructor.
	Params struct {
		Log             *zap.Logger
		PoolSize        int
		NeoFSContract   util.Uint160
		BalanceContract util.Uint160
		MorphClient     *client.Client
		EpochState      EpochState
		ActiveState     ActiveState
	}
)

const (
	depositNotification  = "Deposit"
	withdrawNotification = "Withdraw"
	chequeNotification   = "Cheque"
)

// New creates neofs mainnet contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/neofs: logger is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/neofs: neo:morph client is not set")
	case p.EpochState == nil:
		return nil, errors.New("ir/neofs: global state is not set")
	case p.ActiveState == nil:
		return nil, errors.New("ir/neofs: global state is not set")
	}

	p.Log.Debug("neofs worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, errors.Wrap(err, "ir/neofs: can't create worker pool")
	}

	return &Processor{
		log:             p.Log,
		pool:            pool,
		neofsContract:   p.NeoFSContract,
		balanceContract: p.BalanceContract,
		morphClient:     p.MorphClient,
		epochState:      p.EpochState,
		activeState:     p.ActiveState,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerParsers() []event.ParserInfo {
	var parsers []event.ParserInfo

	// deposit event
	deposit := event.ParserInfo{}
	deposit.SetType(depositNotification)
	deposit.SetScriptHash(np.neofsContract)
	deposit.SetParser(neofsEvent.ParseDeposit)
	parsers = append(parsers, deposit)

	// withdraw event
	withdraw := event.ParserInfo{}
	withdraw.SetType(withdrawNotification)
	withdraw.SetScriptHash(np.neofsContract)
	withdraw.SetParser(neofsEvent.ParseWithdraw)
	parsers = append(parsers, withdraw)

	// cheque event
	cheque := event.ParserInfo{}
	cheque.SetType(chequeNotification)
	cheque.SetScriptHash(np.neofsContract)
	cheque.SetParser(neofsEvent.ParseCheque)
	parsers = append(parsers, cheque)

	return parsers
}

// ListenerHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerHandlers() []event.HandlerInfo {
	var handlers []event.HandlerInfo

	// deposit handler
	deposit := event.HandlerInfo{}
	deposit.SetType(depositNotification)
	deposit.SetScriptHash(np.neofsContract)
	deposit.SetHandler(np.handleDeposit)
	handlers = append(handlers, deposit)

	// withdraw handler
	withdraw := event.HandlerInfo{}
	withdraw.SetType(withdrawNotification)
	withdraw.SetScriptHash(np.neofsContract)
	withdraw.SetHandler(np.handleWithdraw)
	handlers = append(handlers, withdraw)

	// cheque handler
	cheque := event.HandlerInfo{}
	cheque.SetType(chequeNotification)
	cheque.SetScriptHash(np.neofsContract)
	cheque.SetHandler(np.handleCheque)
	handlers = append(handlers, cheque)

	return handlers
}

// TimersHandlers for the 'Timers' event producer.
func (np *Processor) TimersHandlers() []event.HandlerInfo {
	return nil
}
