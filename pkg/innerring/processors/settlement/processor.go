package settlement

import (
	"math/big"

	balanceClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	containerClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	netmapClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"go.uber.org/zap"
)

var (
	bigGB  = big.NewInt(1 << 30)
	bigOne = big.NewInt(1)
)

// parallelFactor is a max parallel routines number doing settlement
// calculations/handlings.
const parallelFactor = 10

type (
	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// Processor is an event handler for payments in the system.
	Processor struct {
		log *zap.Logger

		state AlphabetState

		cnrClient     *containerClient.Client
		nmClient      *netmapClient.Client
		balanceClient *balanceClient.Client
	}

	// Prm groups the required parameters of Processor's constructor.
	Prm struct {
		State           AlphabetState
		ContainerClient *containerClient.Client
		NetmapClient    *netmapClient.Client
		BalanceClient   *balanceClient.Client
	}
)

// New creates and returns a new Processor instance.
func New(prm Prm, opts ...Option) *Processor {
	o := defaultOptions()

	for i := range opts {
		opts[i](o)
	}

	return &Processor{
		log:           o.log,
		state:         prm.State,
		cnrClient:     prm.ContainerClient,
		nmClient:      prm.NetmapClient,
		balanceClient: prm.BalanceClient,
	}
}
