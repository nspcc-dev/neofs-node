package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	neofsid "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// Processor of events produced by container contract in morph chain.
	Processor struct {
		log               *zap.Logger
		pool              *ants.Pool
		containerContract util.Uint160
		morphClient       *client.Client
		alphabetState     AlphabetState
		feeProvider       *config.FeeConfig
		cnrClient         *wrapper.Wrapper // notary must be enabled
		idClient          *neofsid.ClientWrapper
	}

	// Params of the processor constructor.
	Params struct {
		Log               *zap.Logger
		PoolSize          int
		ContainerContract util.Uint160
		MorphClient       *client.Client
		AlphabetState     AlphabetState
		FeeProvider       *config.FeeConfig
		ContainerClient   *wrapper.Wrapper
		NeoFSIDClient     *neofsid.ClientWrapper
	}
)

const (
	putNotification    = "containerPut"
	deleteNotification = "containerDelete"

	setEACLNotification = "setEACL"
)

// New creates container contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/container: logger is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/container: neo:morph client is not set")
	case p.AlphabetState == nil:
		return nil, errors.New("ir/container: global state is not set")
	case p.FeeProvider == nil:
		return nil, errors.New("ir/container: fee provider is not set")
	case p.ContainerClient == nil:
		return nil, errors.New("ir/container: Container client is not set")
	case p.NeoFSIDClient == nil:
		return nil, errors.New("ir/container: NeoFS ID client is not set")
	}

	p.Log.Debug("container worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/container: can't create worker pool: %w", err)
	}

	return &Processor{
		log:               p.Log,
		pool:              pool,
		containerContract: p.ContainerContract,
		morphClient:       p.MorphClient,
		alphabetState:     p.AlphabetState,
		feeProvider:       p.FeeProvider,
		cnrClient:         p.ContainerClient,
		idClient:          p.NeoFSIDClient,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (cp *Processor) ListenerParsers() []event.ParserInfo {
	var (
		parsers = make([]event.ParserInfo, 0, 3)

		p event.ParserInfo
	)

	p.SetScriptHash(cp.containerContract)

	// container put
	p.SetType(event.TypeFromString(putNotification))
	p.SetParser(containerEvent.ParsePut)
	parsers = append(parsers, p)

	// container delete
	p.SetType(event.TypeFromString(deleteNotification))
	p.SetParser(containerEvent.ParseDelete)
	parsers = append(parsers, p)

	// set eACL
	p.SetType(event.TypeFromString(setEACLNotification))
	p.SetParser(containerEvent.ParseSetEACL)
	parsers = append(parsers, p)

	return parsers
}

// ListenerHandlers for the 'event.Listener' event producer.
func (cp *Processor) ListenerHandlers() []event.HandlerInfo {
	var (
		handlers = make([]event.HandlerInfo, 0, 3)

		h event.HandlerInfo
	)

	h.SetScriptHash(cp.containerContract)

	// container put
	h.SetType(event.TypeFromString(putNotification))
	h.SetHandler(cp.handlePut)
	handlers = append(handlers, h)

	// container delete
	h.SetType(event.TypeFromString(deleteNotification))
	h.SetHandler(cp.handleDelete)
	handlers = append(handlers, h)

	// set eACL
	h.SetType(event.TypeFromString(setEACLNotification))
	h.SetHandler(cp.handleSetEACL)
	handlers = append(handlers, h)

	return handlers
}

// TimersHandlers for the 'Timers' event producer.
func (cp *Processor) TimersHandlers() []event.HandlerInfo {
	return nil
}
