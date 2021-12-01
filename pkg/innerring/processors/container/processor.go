package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	neofsid "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid/wrapper"
	morphsubnet "github.com/nspcc-dev/neofs-node/pkg/morph/client/subnet"
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
		log            *zap.Logger
		pool           *ants.Pool
		alphabetState  AlphabetState
		cnrClient      *wrapper.Wrapper // notary must be enabled
		idClient       *neofsid.ClientWrapper
		subnetClient   *morphsubnet.Client
		netState       NetworkState
		notaryDisabled bool
	}

	// Params of the processor constructor.
	Params struct {
		Log             *zap.Logger
		PoolSize        int
		AlphabetState   AlphabetState
		ContainerClient *wrapper.Wrapper
		NeoFSIDClient   *neofsid.ClientWrapper
		SubnetClient    *morphsubnet.Client
		NetworkState    NetworkState
		NotaryDisabled  bool
	}
)

// NetworkState is an interface of a component
// that provides access to network state.
type NetworkState interface {
	// Epoch must return number of the current epoch.
	//
	// Must return any error encountered
	// which did not allow reading the value.
	Epoch() (uint64, error)
}

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
	case p.AlphabetState == nil:
		return nil, errors.New("ir/container: global state is not set")
	case p.ContainerClient == nil:
		return nil, errors.New("ir/container: Container client is not set")
	case p.NeoFSIDClient == nil:
		return nil, errors.New("ir/container: NeoFS ID client is not set")
	case p.NetworkState == nil:
		return nil, errors.New("ir/container: network state is not set")
	case p.SubnetClient == nil:
		return nil, errors.New("ir/container: subnet client is not set")
	}

	p.Log.Debug("container worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/container: can't create worker pool: %w", err)
	}

	return &Processor{
		log:            p.Log,
		pool:           pool,
		alphabetState:  p.AlphabetState,
		cnrClient:      p.ContainerClient,
		idClient:       p.NeoFSIDClient,
		netState:       p.NetworkState,
		notaryDisabled: p.NotaryDisabled,
		subnetClient:   p.SubnetClient,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (cp *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	if !cp.notaryDisabled {
		return nil
	}

	var (
		parsers = make([]event.NotificationParserInfo, 0, 3)

		p event.NotificationParserInfo
	)

	p.SetScriptHash(cp.cnrClient.ContractAddress())

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

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (cp *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	if !cp.notaryDisabled {
		return nil
	}

	var (
		handlers = make([]event.NotificationHandlerInfo, 0, 3)

		h event.NotificationHandlerInfo
	)

	h.SetScriptHash(cp.cnrClient.ContractAddress())

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

// ListenerNotaryParsers for the 'event.Listener' notary event producer.
func (cp *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	var (
		p event.NotaryParserInfo

		pp = make([]event.NotaryParserInfo, 0, 4)
	)

	p.SetMempoolType(mempoolevent.TransactionAdded)
	p.SetScriptHash(cp.cnrClient.ContractAddress())

	// container put
	p.SetRequestType(containerEvent.PutNotaryEvent)
	p.SetParser(containerEvent.ParsePutNotary)
	pp = append(pp, p)

	// container named put
	p.SetRequestType(containerEvent.PutNamedNotaryEvent)
	p.SetParser(containerEvent.ParsePutNamedNotary)
	pp = append(pp, p)

	// container delete
	p.SetRequestType(containerEvent.DeleteNotaryEvent)
	p.SetParser(containerEvent.ParseDeleteNotary)
	pp = append(pp, p)

	// set EACL
	p.SetRequestType(containerEvent.SetEACLNotaryEvent)
	p.SetParser(containerEvent.ParseSetEACLNotary)
	pp = append(pp, p)

	return pp
}

// ListenerNotaryHandlers for the 'event.Listener' notary event producer.
func (cp *Processor) ListenerNotaryHandlers() []event.NotaryHandlerInfo {
	var (
		h event.NotaryHandlerInfo

		hh = make([]event.NotaryHandlerInfo, 0, 4)
	)

	h.SetScriptHash(cp.cnrClient.ContractAddress())
	h.SetMempoolType(mempoolevent.TransactionAdded)

	// container put
	h.SetRequestType(containerEvent.PutNotaryEvent)
	h.SetHandler(cp.handlePut)
	hh = append(hh, h)

	// container named put (same handler)
	h.SetRequestType(containerEvent.PutNamedNotaryEvent)
	hh = append(hh, h)

	// container delete
	h.SetRequestType(containerEvent.DeleteNotaryEvent)
	h.SetHandler(cp.handleDelete)
	hh = append(hh, h)

	// set eACL
	h.SetRequestType(containerEvent.SetEACLNotaryEvent)
	h.SetHandler(cp.handleSetEACL)
	hh = append(hh, h)

	return hh
}

// TimersHandlers for the 'Timers' event producer.
func (cp *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}
