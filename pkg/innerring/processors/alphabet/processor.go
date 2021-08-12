package alphabet

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	nmWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// Indexer is a callback interface for inner ring global state.
	Indexer interface {
		AlphabetIndex() int
	}

	// Contracts is an interface of the storage
	// of the alphabet contract addresses.
	Contracts interface {
		// GetByIndex must return address of the
		// alphabet contract by index of the glagolitic
		// letter (e.g 0 for Az, 40 for Izhitsa).
		//
		// Must return false if index does not
		// match to any alphabet contract.
		GetByIndex(int) (util.Uint160, bool)
	}

	// Processor of events produced for alphabet contracts in sidechain.
	Processor struct {
		log               *zap.Logger
		pool              *ants.Pool
		alphabetContracts Contracts
		netmapClient      *nmWrapper.Wrapper
		morphClient       *client.Client
		irList            Indexer
		storageEmission   uint64
	}

	// Params of the processor constructor.
	Params struct {
		Log               *zap.Logger
		PoolSize          int
		AlphabetContracts Contracts
		NetmapClient      *nmWrapper.Wrapper
		MorphClient       *client.Client
		IRList            Indexer
		StorageEmission   uint64
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
		return nil, fmt.Errorf("ir/neofs: can't create worker pool: %w", err)
	}

	return &Processor{
		log:               p.Log,
		pool:              pool,
		alphabetContracts: p.AlphabetContracts,
		netmapClient:      p.NetmapClient,
		morphClient:       p.MorphClient,
		irList:            p.IRList,
		storageEmission:   p.StorageEmission,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	return nil
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (np *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}
