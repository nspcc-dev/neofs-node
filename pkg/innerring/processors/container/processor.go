package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// Processor of events produced by container contract in FS chain.
	Processor struct {
		log           *zap.Logger
		pool          *ants.Pool
		alphabetState AlphabetState
		cnrClient     *container.Client // notary must be enabled
		idClient      *neofsid.Client
		netState      NetworkState
		metaEnabled   bool
	}

	// Params of the processor constructor.
	Params struct {
		Log             *zap.Logger
		PoolSize        int
		AlphabetState   AlphabetState
		ContainerClient *container.Client
		NeoFSIDClient   *neofsid.Client
		NetworkState    NetworkState
		MetaEnabled     bool
	}
)

// NetworkState is an interface of a component
// that provides access to network state.
type NetworkState interface {
	// Epoch must return the number of the current epoch.
	//
	// Must return any error encountered
	// which did not allow reading the value.
	Epoch() (uint64, error)

	// HomomorphicHashDisabled must return boolean that
	// represents homomorphic network state:
	// 	* true if hashing is disabled;
	// 	* false if hashing is enabled.
	//
	// which did not allow reading the value.
	HomomorphicHashDisabled() (bool, error)

	// NetMap must return actual network map.
	NetMap() (*netmap.NetMap, error)
}

// New creates a container contract processor instance.
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
	}

	p.Log.Debug("container worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/container: can't create worker pool: %w", err)
	}

	return &Processor{
		log:           p.Log,
		pool:          pool,
		alphabetState: p.AlphabetState,
		cnrClient:     p.ContainerClient,
		idClient:      p.NeoFSIDClient,
		netState:      p.NetworkState,
		metaEnabled:   p.MetaEnabled,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (cp *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	return nil
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (cp *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	return nil
}

// ListenerNotaryParsers for the 'event.Listener' notary event producer.
func (cp *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	var (
		p event.NotaryParserInfo

		pp = make([]event.NotaryParserInfo, 0, 4)
	)

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
