package balance

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	balanceEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/balance"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// AlphabetState is a callback interface for inner ring global state
	AlphabetState interface {
		IsAlphabet() bool
	}

	// PrecisionConverter converts balance amount values.
	PrecisionConverter interface {
		ToFixed8(int64) int64
	}

	// Processor of events produced by balance contract in the morphchain.
	Processor struct {
		log           *zap.Logger
		pool          *ants.Pool
		neofsClient   *neofscontract.Client
		balanceSC     util.Uint160
		alphabetState AlphabetState
		converter     PrecisionConverter
	}

	// Params of the processor constructor.
	Params struct {
		Log           *zap.Logger
		PoolSize      int
		NeoFSClient   *neofscontract.Client
		BalanceSC     util.Uint160
		AlphabetState AlphabetState
		Converter     PrecisionConverter
	}
)

const (
	lockNotification = "Lock"
)

// New creates a balance contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/balance: logger is not set")
	case p.AlphabetState == nil:
		return nil, errors.New("ir/balance: global state is not set")
	case p.Converter == nil:
		return nil, errors.New("ir/balance: balance precision converter is not set")
	}

	p.Log.Debug("balance worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/balance: can't create worker pool: %w", err)
	}

	return &Processor{
		log:           p.Log,
		pool:          pool,
		neofsClient:   p.NeoFSClient,
		balanceSC:     p.BalanceSC,
		alphabetState: p.AlphabetState,
		converter:     p.Converter,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (bp *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	var parsers []event.NotificationParserInfo

	// new lock event
	lock := event.NotificationParserInfo{}
	lock.SetType(lockNotification)
	lock.SetScriptHash(bp.balanceSC)
	lock.SetParser(balanceEvent.ParseLock)
	parsers = append(parsers, lock)

	return parsers
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (bp *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	var handlers []event.NotificationHandlerInfo

	// lock handler
	lock := event.NotificationHandlerInfo{}
	lock.SetType(lockNotification)
	lock.SetScriptHash(bp.balanceSC)
	lock.SetHandler(bp.handleLock)
	handlers = append(handlers, lock)

	return handlers
}

// ListenerNotaryParsers for the 'event.Listener' event producer.
func (bp *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	return nil
}

// ListenerNotaryHandlers for the 'event.Listener' event producer.
func (bp *Processor) ListenerNotaryHandlers() []event.NotaryHandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (bp *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}
