package alphabet

import (
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// Indexer is a callback interface for inner ring global state.
	Indexer interface {
		AlphabetIndex() int
	}

	// Processor of events produced for alphabet contracts in FS chain.
	Processor struct {
		log               *zap.Logger
		pool              *ants.Pool
		alphabetContracts []util.Uint160
		netmapClient      *nmClient.Client
		fsChainClient     *client.Client
		irList            Indexer
		storageEmission   uint64
	}

	// Params of the processor constructor.
	Params struct {
		Log               *zap.Logger
		PoolSize          int
		AlphabetContracts []util.Uint160
		NetmapClient      *nmClient.Client
		FSChainClient     *client.Client
		IRList            Indexer
		StorageEmission   uint64
	}
)

// New creates a neofs mainnet contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/alphabet: logger is not set")
	case p.FSChainClient == nil:
		return nil, errors.New("ir/alphabet: neo: fs chain client is not set")
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
		alphabetContracts: slices.Clone(p.AlphabetContracts),
		netmapClient:      p.NetmapClient,
		fsChainClient:     p.FSChainClient,
		irList:            p.IRList,
		storageEmission:   p.StorageEmission,
	}, nil
}

// newEpochNotification is the new epoch event name.
const newEpochNotification = "NewEpoch"

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (ap *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	parsers := make([]event.NotificationParserInfo, 0, 1)

	var p event.NotificationParserInfo
	p.SetScriptHash(ap.netmapClient.ContractAddress())

	// new epoch event
	p.SetType(newEpochNotification)
	p.SetParser(netmapEvent.ParseNewEpoch)
	parsers = append(parsers, p)

	return parsers
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (ap *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	handlers := make([]event.NotificationHandlerInfo, 0, 1)

	var i event.NotificationHandlerInfo
	i.SetScriptHash(ap.netmapClient.ContractAddress())

	// new epoch handler
	i.SetType(newEpochNotification)
	i.SetHandler(ap.HandleGasEmission)
	handlers = append(handlers, i)

	return handlers
}

// ListenerNotaryParsers for the 'event.Listener' event producer.
func (ap *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	return nil
}

// ListenerNotaryHandlers for the 'event.Listener' event producer.
func (ap *Processor) ListenerNotaryHandlers() []event.NotaryHandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (ap *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}
