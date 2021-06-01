package balance

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs/wrapper"
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

	// Processor of events produced by balance contract in morph chain.
	Processor struct {
		log             *zap.Logger
		pool            *ants.Pool
		neofsClient     *neofscontract.ClientWrapper
		balanceContract util.Uint160
		alphabetState   AlphabetState
		converter       PrecisionConverter
	}

	// Params of the processor constructor.
	Params struct {
		Log             *zap.Logger
		PoolSize        int
		NeoFSClient     *neofscontract.ClientWrapper
		BalanceContract util.Uint160
		AlphabetState   AlphabetState
		Converter       PrecisionConverter
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
		log:             p.Log,
		pool:            pool,
		neofsClient:     p.NeoFSClient,
		balanceContract: p.BalanceContract,
		alphabetState:   p.AlphabetState,
		converter:       p.Converter,
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
