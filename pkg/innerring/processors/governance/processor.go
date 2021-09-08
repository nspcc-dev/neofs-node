package governance

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs/wrapper"
	nmWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/rolemanagement"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// ProcessorPoolSize limits pool size for governance Processor. Processor manages
// governance sync tasks. This process must not be interrupted by other sync
// operation, so we limit pool size for processor to one.
const ProcessorPoolSize = 1

type (
	// AlphabetState is a callback interface for innerring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// Voter is a callback interface for alphabet contract voting.
	Voter interface {
		VoteForSidechainValidator(keys keys.PublicKeys) error
	}

	// EpochState is a callback interface for innerring global state.
	EpochState interface {
		EpochCounter() uint64
	}

	// IRFetcher is a callback interface for innerring keys.
	// Implementation must take into account availability of
	// the notary contract.
	IRFetcher interface {
		InnerRingKeys() (keys.PublicKeys, error)
	}

	// Processor of events related to governance in the network.
	Processor struct {
		log          *zap.Logger
		pool         *ants.Pool
		neofsClient  *neofscontract.ClientWrapper
		netmapClient *nmWrapper.Wrapper

		alphabetState AlphabetState
		epochState    EpochState
		voter         Voter
		irFetcher     IRFetcher

		mainnetClient *client.Client
		morphClient   *client.Client

		notaryDisabled bool

		designate util.Uint160
	}

	// Params of the processor constructor.
	Params struct {
		Log *zap.Logger

		AlphabetState AlphabetState
		EpochState    EpochState
		Voter         Voter
		IRFetcher     IRFetcher

		MorphClient   *client.Client
		MainnetClient *client.Client
		NeoFSClient   *neofscontract.ClientWrapper
		NetmapClient  *nmWrapper.Wrapper

		NotaryDisabled bool
	}
)

// New creates balance contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/governance: logger is not set")
	case p.MainnetClient == nil:
		return nil, errors.New("ir/governance: neo:mainnet client is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/governance: neo:sidechain client is not set")
	case p.AlphabetState == nil:
		return nil, errors.New("ir/governance: global state is not set")
	case p.EpochState == nil:
		return nil, errors.New("ir/governance: global state is not set")
	case p.Voter == nil:
		return nil, errors.New("ir/governance: global state is not set")
	case p.IRFetcher == nil:
		return nil, errors.New("ir/governance: innerring keys fetcher is not set")
	}

	pool, err := ants.NewPool(ProcessorPoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/governance: can't create worker pool: %w", err)
	}

	// result is cached by neo-go, so we can pre-calc it
	designate, err := p.MainnetClient.GetDesignateHash()
	if err != nil {
		return nil, fmt.Errorf("could not get designate hash: %w", err)
	}

	return &Processor{
		log:            p.Log,
		pool:           pool,
		neofsClient:    p.NeoFSClient,
		netmapClient:   p.NetmapClient,
		alphabetState:  p.AlphabetState,
		epochState:     p.EpochState,
		voter:          p.Voter,
		irFetcher:      p.IRFetcher,
		mainnetClient:  p.MainnetClient,
		morphClient:    p.MorphClient,
		notaryDisabled: p.NotaryDisabled,
		designate:      designate,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (gp *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	var pi event.NotificationParserInfo
	pi.SetScriptHash(gp.designate)
	pi.SetType(event.TypeFromString(native.DesignationEventName))
	pi.SetParser(rolemanagement.ParseDesignate)
	return []event.NotificationParserInfo{pi}
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (gp *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	var hi event.NotificationHandlerInfo
	hi.SetScriptHash(gp.designate)
	hi.SetType(event.TypeFromString(native.DesignationEventName))
	hi.SetHandler(gp.HandleAlphabetSync)
	return []event.NotificationHandlerInfo{hi}
}

// ListenerNotaryParsers for the 'event.Listener' event producer.
func (gp *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	return nil
}

// ListenerNotaryHandlers for the 'event.Listener' event producer.
func (gp *Processor) ListenerNotaryHandlers() []event.NotaryHandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (gp *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}
