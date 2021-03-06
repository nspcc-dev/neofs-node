package container

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// ActiveState is a callback interface for inner ring global state.
	ActiveState interface {
		IsActive() bool
	}

	// Processor of events produced by container contract in morph chain.
	Processor struct {
		log               *zap.Logger
		pool              *ants.Pool
		containerContract util.Uint160
		morphClient       *client.Client
		activeState       ActiveState
	}

	// Params of the processor constructor.
	Params struct {
		Log               *zap.Logger
		PoolSize          int
		ContainerContract util.Uint160
		MorphClient       *client.Client
		ActiveState       ActiveState
	}
)

const (
	putNotification    = "containerPut"
	deleteNotification = "containerDelete"
)

// New creates container contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/container: logger is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/container: neo:morph client is not set")
	case p.ActiveState == nil:
		return nil, errors.New("ir/container: global state is not set")
	}

	p.Log.Debug("container worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, errors.Wrap(err, "ir/container: can't create worker pool")
	}

	return &Processor{
		log:               p.Log,
		pool:              pool,
		containerContract: p.ContainerContract,
		morphClient:       p.MorphClient,
		activeState:       p.ActiveState,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (cp *Processor) ListenerParsers() []event.ParserInfo {
	var parsers []event.ParserInfo

	// container put event
	put := event.ParserInfo{}
	put.SetType(putNotification)
	put.SetScriptHash(cp.containerContract)
	put.SetParser(containerEvent.ParsePut)
	parsers = append(parsers, put)

	// container del event
	del := event.ParserInfo{}
	del.SetType(deleteNotification)
	del.SetScriptHash(cp.containerContract)
	del.SetParser(containerEvent.ParseDelete)
	parsers = append(parsers, del)

	return parsers
}

// ListenerHandlers for the 'event.Listener' event producer.
func (cp *Processor) ListenerHandlers() []event.HandlerInfo {
	var handlers []event.HandlerInfo

	// container put handler
	put := event.HandlerInfo{}
	put.SetType(putNotification)
	put.SetScriptHash(cp.containerContract)
	put.SetHandler(cp.handlePut)
	handlers = append(handlers, put)

	del := event.HandlerInfo{}
	del.SetType(deleteNotification)
	del.SetScriptHash(cp.containerContract)
	del.SetHandler(cp.handleDelete)
	handlers = append(handlers, del)

	return handlers
}

// TimersHandlers for the 'Timers' event producer.
func (cp *Processor) TimersHandlers() []event.HandlerInfo {
	return nil
}
