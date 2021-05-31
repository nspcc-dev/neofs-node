package governance

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	nmWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/rolemanagement"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// GovernanceProcessor manages governance sync tasks. This process must not be
// interrupted by other sync operation, so we limit pool size for processor to
// one.
const ProcessorPoolSize = 1

type (
	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// Voter is a callback interface for alphabet contract voting.
	Voter interface {
		VoteForSidechainValidator(keys keys.PublicKeys) error
	}

	// EpochState is a callback interface for inner ring global state.
	EpochState interface {
		EpochCounter() uint64
	}

	// Processor of events related to governance in the network.
	Processor struct {
		log           *zap.Logger
		pool          *ants.Pool
		neofsContract util.Uint160
		netmapClient  *nmWrapper.Wrapper

		alphabetState AlphabetState
		epochState    EpochState
		voter         Voter

		mainnetClient *client.Client
		morphClient   *client.Client

		notaryDisabled bool
		feeProvider    *config.FeeConfig
	}

	// Params of the processor constructor.
	Params struct {
		Log           *zap.Logger
		NeoFSContract util.Uint160

		AlphabetState AlphabetState
		EpochState    EpochState
		Voter         Voter

		MorphClient   *client.Client
		MainnetClient *client.Client
		NetmapClient  *nmWrapper.Wrapper

		NotaryDisabled bool
		FeeProvider    *config.FeeConfig
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
	case p.FeeProvider == nil:
		return nil, errors.New("ir/governance: fee provider is not set")
	}

	pool, err := ants.NewPool(ProcessorPoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/governance: can't create worker pool: %w", err)
	}

	return &Processor{
		log:            p.Log,
		pool:           pool,
		neofsContract:  p.NeoFSContract,
		netmapClient:   p.NetmapClient,
		alphabetState:  p.AlphabetState,
		epochState:     p.EpochState,
		voter:          p.Voter,
		mainnetClient:  p.MainnetClient,
		morphClient:    p.MorphClient,
		notaryDisabled: p.NotaryDisabled,
		feeProvider:    p.FeeProvider,
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (gp *Processor) ListenerParsers() []event.ParserInfo {
	var pi event.ParserInfo
	pi.SetScriptHash(gp.mainnetClient.GetDesignateHash())
	pi.SetType(event.TypeFromString(native.DesignationEventName))
	pi.SetParser(rolemanagement.ParseDesignate)
	return []event.ParserInfo{pi}
}

// ListenerHandlers for the 'event.Listener' event producer.
func (gp *Processor) ListenerHandlers() []event.HandlerInfo {
	var hi event.HandlerInfo
	hi.SetScriptHash(gp.mainnetClient.GetDesignateHash())
	hi.SetType(event.TypeFromString(native.DesignationEventName))
	hi.SetHandler(gp.HandleAlphabetSync)
	return []event.HandlerInfo{hi}
}

// TimersHandlers for the 'Timers' event producer.
func (gp *Processor) TimersHandlers() []event.HandlerInfo {
	return nil
}
