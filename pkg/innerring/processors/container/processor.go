package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// NodeState provides current state of the Inner Ring member in the NeoFS
// network.
type NodeState interface {
	// IsAlphabet checks if the node belongs to the Alphabet set. IsAlphabet
	// MUST return false if the assertion cannot be verified for some reason.
	IsAlphabet() bool
}

// AuthSystem is an authentication/authorization system in the NeoFS network.
type AuthSystem interface {
	// VerifySignature performs user authorization by checking the signature
	// of the given data. The user is authenticated using the optional
	// neofsecdsa.PublicKeyRFC6979 parameter. If key is not provided,
	// VerifySignature authenticates the user internally. VerifySignature
	// returns no error if and only if authorization is passed successfully.
	VerifySignature(usr user.ID, data []byte, signature []byte, key *neofsecdsa.PublicKeyRFC6979) error
}

// Subnets represents mechanism of organization of NeoFS network parties
// in a subnet.
type Subnets interface {
	// ContainerOwnerAllowed checks if container creator is allowed to bind
	// containers to the given subnet. ContainerOwnerAllowed MUST return false
	// allowance cannot be verified for any reason.
	ContainerOwnerAllowed(subnetid.ID, user.ID) bool
}

type (
	// Processor of events produced by container contract in the sidechain.
	Processor struct {
		log            *logger.Logger
		pool           *ants.Pool
		nodeState      NodeState
		cnrClient      *container.Client // notary must be enabled
		authSystem     AuthSystem
		subnets        Subnets
		netState       NetworkState
		notaryDisabled bool
	}

	// Params of the processor constructor.
	Params struct {
		Log             *logger.Logger
		PoolSize        int
		NodeState       NodeState
		ContainerClient *container.Client
		AuthSystem      AuthSystem
		Subnets         Subnets
		NetworkState    NetworkState
		NotaryDisabled  bool
	}
)

// NetworkState provides current state of the NeoFS network.
type NetworkState interface {
	// Epoch returns the number of the current epoch. Returns any error encountered
	// which did not allow to determine the epoch.
	Epoch() (uint64, error)

	// HomomorphicHashDisabled checks if homomorphic hashing of the object data
	// is disabled. Returns any error encountered which did not allow to determine
	// the property.
	HomomorphicHashDisabled() (bool, error)
}

const (
	putNotification    = "containerPut"
	deleteNotification = "containerDelete"

	setEACLNotification = "setEACL"
)

// New creates a container contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/container: logger is not set")
	case p.NodeState == nil:
		return nil, errors.New("ir/container: global state is not set")
	case p.ContainerClient == nil:
		return nil, errors.New("ir/container: Container client is not set")
	case p.AuthSystem == nil:
		return nil, errors.New("ir/container: auth system is not set")
	case p.NetworkState == nil:
		return nil, errors.New("ir/container: network state is not set")
	case p.Subnets == nil:
		return nil, errors.New("ir/container: subnets mechanism is not set")
	}

	p.Log.Debug("container worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/container: can't create worker pool: %w", err)
	}

	return &Processor{
		log:            p.Log,
		pool:           pool,
		nodeState:      p.NodeState,
		cnrClient:      p.ContainerClient,
		authSystem:     p.AuthSystem,
		netState:       p.NetworkState,
		notaryDisabled: p.NotaryDisabled,
		subnets:        p.Subnets,
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
