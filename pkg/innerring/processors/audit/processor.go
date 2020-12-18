package audit

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
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

	// Processor of events related with data audit.
	Processor struct {
		log               *zap.Logger
		pool              *ants.Pool
		containerContract util.Uint160
		auditContract     util.Uint160
		morphClient       *client.Client
		irList            Indexer
	}

	// Params of the processor constructor.
	Params struct {
		Log               *zap.Logger
		ContainerContract util.Uint160
		AuditContract     util.Uint160
		MorphClient       *client.Client
		IRList            Indexer
	}
)

// AuditProcessor manages audit tasks and fills queue for next epoch. This
// process must not be interrupted by new audit epoch, so we limit pool size
// for processor to one.
const ProcessorPoolSize = 1

// New creates audit processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/audit: logger is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/audit: neo:morph client is not set")
	case p.IRList == nil:
		return nil, errors.New("ir/audit: global state is not set")
	}

	pool, err := ants.NewPool(ProcessorPoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, errors.Wrap(err, "ir/audit: can't create worker pool")
	}

	return &Processor{
		log:               p.Log,
		pool:              pool,
		containerContract: p.ContainerContract,
		auditContract:     p.AuditContract,
		morphClient:       p.MorphClient,
		irList:            p.IRList,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (ap *Processor) ListenerParsers() []event.ParserInfo {
	return nil
}

// ListenerHandlers for the 'event.Listener' event producer.
func (ap *Processor) ListenerHandlers() []event.HandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (ap *Processor) TimersHandlers() []event.HandlerInfo {
	return nil
}

// StartAuditHandler for the internal event producer.
func (ap *Processor) StartAuditHandler() event.Handler {
	return ap.handleNewAuditRound
}
