package neofs

import (
	"errors"
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	balanceWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	neofsid "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid/wrapper"
	nmWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// EpochState is a callback interface for inner ring global state
	EpochState interface {
		EpochCounter() uint64
	}

	// AlphabetState is a callback interface for inner ring global state
	AlphabetState interface {
		IsAlphabet() bool
	}

	// PrecisionConverter converts balance amount values.
	PrecisionConverter interface {
		ToBalancePrecision(int64) int64
	}

	// Processor of events produced by neofs contract in main net.
	Processor struct {
		log                 *zap.Logger
		pool                *ants.Pool
		neofsContract       util.Uint160
		balanceClient       *balanceWrapper.Wrapper
		netmapClient        *nmWrapper.Wrapper
		morphClient         *client.Client
		epochState          EpochState
		alphabetState       AlphabetState
		converter           PrecisionConverter
		mintEmitLock        *sync.Mutex
		mintEmitCache       *lru.Cache
		mintEmitThreshold   uint64
		mintEmitValue       fixedn.Fixed8
		gasBalanceThreshold int64

		neofsIDClient *neofsid.ClientWrapper
	}

	// Params of the processor constructor.
	Params struct {
		Log                 *zap.Logger
		PoolSize            int
		NeoFSContract       util.Uint160
		NeoFSIDClient       *neofsid.ClientWrapper
		BalanceClient       *balanceWrapper.Wrapper
		NetmapClient        *nmWrapper.Wrapper
		MorphClient         *client.Client
		EpochState          EpochState
		AlphabetState       AlphabetState
		Converter           PrecisionConverter
		MintEmitCacheSize   int
		MintEmitThreshold   uint64 // in epochs
		MintEmitValue       fixedn.Fixed8
		GasBalanceThreshold int64
	}
)

const (
	depositNotification  = "Deposit"
	withdrawNotification = "Withdraw"
	chequeNotification   = "Cheque"
	configNotification   = "SetConfig"
	bindNotification     = "Bind"
	unbindNotification   = "Unbind"
)

// New creates neofs mainnet contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/neofs: logger is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/neofs: neo:morph client is not set")
	case p.EpochState == nil:
		return nil, errors.New("ir/neofs: global state is not set")
	case p.AlphabetState == nil:
		return nil, errors.New("ir/neofs: global state is not set")
	case p.Converter == nil:
		return nil, errors.New("ir/neofs: balance precision converter is not set")
	}

	p.Log.Debug("neofs worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/neofs: can't create worker pool: %w", err)
	}

	lruCache, err := lru.New(p.MintEmitCacheSize)
	if err != nil {
		return nil, fmt.Errorf("ir/neofs: can't create LRU cache for gas emission: %w", err)
	}

	return &Processor{
		log:                 p.Log,
		pool:                pool,
		neofsContract:       p.NeoFSContract,
		balanceClient:       p.BalanceClient,
		netmapClient:        p.NetmapClient,
		morphClient:         p.MorphClient,
		epochState:          p.EpochState,
		alphabetState:       p.AlphabetState,
		converter:           p.Converter,
		mintEmitLock:        new(sync.Mutex),
		mintEmitCache:       lruCache,
		mintEmitThreshold:   p.MintEmitThreshold,
		mintEmitValue:       p.MintEmitValue,
		gasBalanceThreshold: p.GasBalanceThreshold,

		neofsIDClient: p.NeoFSIDClient,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	var (
		parsers = make([]event.NotificationParserInfo, 0, 6)

		p event.NotificationParserInfo
	)

	p.SetScriptHash(np.neofsContract)

	// deposit event
	p.SetType(event.TypeFromString(depositNotification))
	p.SetParser(neofsEvent.ParseDeposit)
	parsers = append(parsers, p)

	// withdraw event
	p.SetType(event.TypeFromString(withdrawNotification))
	p.SetParser(neofsEvent.ParseWithdraw)
	parsers = append(parsers, p)

	// cheque event
	p.SetType(event.TypeFromString(chequeNotification))
	p.SetParser(neofsEvent.ParseCheque)
	parsers = append(parsers, p)

	// config event
	p.SetType(event.TypeFromString(configNotification))
	p.SetParser(neofsEvent.ParseConfig)
	parsers = append(parsers, p)

	// bind event
	p.SetType(event.TypeFromString(bindNotification))
	p.SetParser(neofsEvent.ParseBind)
	parsers = append(parsers, p)

	// unbind event
	p.SetType(event.TypeFromString(unbindNotification))
	p.SetParser(neofsEvent.ParseUnbind)
	parsers = append(parsers, p)

	return parsers
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	var (
		handlers = make([]event.NotificationHandlerInfo, 0, 6)

		h event.NotificationHandlerInfo
	)

	h.SetScriptHash(np.neofsContract)

	// deposit handler
	h.SetType(event.TypeFromString(depositNotification))
	h.SetHandler(np.handleDeposit)
	handlers = append(handlers, h)

	// withdraw handler
	h.SetType(event.TypeFromString(withdrawNotification))
	h.SetHandler(np.handleWithdraw)
	handlers = append(handlers, h)

	// cheque handler
	h.SetType(event.TypeFromString(chequeNotification))
	h.SetHandler(np.handleCheque)
	handlers = append(handlers, h)

	// config handler
	h.SetType(event.TypeFromString(configNotification))
	h.SetHandler(np.handleConfig)
	handlers = append(handlers, h)

	// bind handler
	h.SetType(event.TypeFromString(bindNotification))
	h.SetHandler(np.handleBind)
	handlers = append(handlers, h)

	// unbind handler
	h.SetType(event.TypeFromString(unbindNotification))
	h.SetHandler(np.handleUnbind)
	handlers = append(handlers, h)

	return handlers
}

// ListenerNotaryParsers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	return nil
}

// ListenerNotaryHandlers for the 'event.Listener' event producer.
func (np *Processor) ListenerNotaryHandlers() []event.NotaryHandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (np *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}
