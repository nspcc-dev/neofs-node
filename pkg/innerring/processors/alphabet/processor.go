package alphabet

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// Indexer is a callback interface for inner ring global state.
	Indexer interface {
		Index() int32
	}

	// Processor of events produced for alphabet contracts in sidechain.
	Processor struct {
		log               *zap.Logger
		pool              *ants.Pool
		alphabetContracts [7]util.Uint160
		morphClient       *client.Client
		irList            Indexer
	}

	// Params of the processor constructor.
	Params struct {
		Log               *zap.Logger
		PoolSize          int
		AlphabetContracts [7]util.Uint160
		MorphClient       *client.Client
		IRList            Indexer
	}
)

// New creates neofs mainnet contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/alphabet: logger is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/alphabet: neo:morph client is not set")
	case p.IRList == nil:
		return nil, errors.New("ir/alphabet: global state is not set")
	}

	p.Log.Debug("alphabet worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, errors.Wrap(err, "ir/neofs: can't create worker pool")
	}

	return &Processor{
		log:               p.Log,
		pool:              pool,
		alphabetContracts: p.AlphabetContracts,
		morphClient:       p.MorphClient,
		irList:            p.IRList,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerParsers() []event.ParserInfo {
	return nil
}

// ListenerHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerHandlers() []event.HandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (np *Processor) TimersHandlers() []event.HandlerInfo {
	var handlers []event.HandlerInfo

	// new epoch handler
	newEpoch := event.HandlerInfo{}
	newEpoch.SetType(timers.AlphabetTimer)
	newEpoch.SetHandler(np.handleGasEmission)
	handlers = append(handlers, newEpoch)

	return handlers
}
