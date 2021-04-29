package reputation

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	reputationWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	reputationEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/reputation"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// EpochState is a callback interface for inner ring global state.
	EpochState interface {
		EpochCounter() uint64
	}

	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// Processor of events produced by reputation contract.
	Processor struct {
		log  *zap.Logger
		pool *ants.Pool

		notaryDisabled bool

		reputationContract util.Uint160

		epochState    EpochState
		alphabetState AlphabetState

		reputationWrp *reputationWrapper.ClientWrapper
	}

	// Params of the processor constructor.
	Params struct {
		Log                *zap.Logger
		PoolSize           int
		NotaryDisabled     bool
		ReputationContract util.Uint160
		EpochState         EpochState
		AlphabetState      AlphabetState
		ReputationWrapper  *reputationWrapper.ClientWrapper
	}
)

const (
	putReputationNotification = "reputationPut"
)

// New creates reputation contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/reputation: logger is not set")
	case p.EpochState == nil:
		return nil, errors.New("ir/reputation: global state is not set")
	case p.AlphabetState == nil:
		return nil, errors.New("ir/reputation: global state is not set")
	case p.ReputationWrapper == nil:
		return nil, errors.New("ir/reputation: reputation contract wrapper is not set")
	}

	p.Log.Debug("reputation worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, errors.Wrap(err, "ir/reputation: can't create worker pool")
	}

	return &Processor{
		log:                p.Log,
		pool:               pool,
		notaryDisabled:     p.NotaryDisabled,
		reputationContract: p.ReputationContract,
		epochState:         p.EpochState,
		alphabetState:      p.AlphabetState,
		reputationWrp:      p.ReputationWrapper,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (rp *Processor) ListenerParsers() []event.ParserInfo {
	var parsers []event.ParserInfo

	// put reputation event
	put := event.ParserInfo{}
	put.SetType(putReputationNotification)
	put.SetScriptHash(rp.reputationContract)
	put.SetParser(reputationEvent.ParsePut)
	parsers = append(parsers, put)

	return parsers
}

// ListenerHandlers for the 'event.Listener' event producer.
func (rp *Processor) ListenerHandlers() []event.HandlerInfo {
	var handlers []event.HandlerInfo

	// put reputation handler
	put := event.HandlerInfo{}
	put.SetType(putReputationNotification)
	put.SetScriptHash(rp.reputationContract)
	put.SetHandler(rp.handlePutReputation)
	handlers = append(handlers, put)

	return handlers
}

// TimersHandlers for the 'Timers' event producer.
func (rp *Processor) TimersHandlers() []event.HandlerInfo {
	return nil
}
