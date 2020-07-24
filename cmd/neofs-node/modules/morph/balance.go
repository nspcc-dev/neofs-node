package morph

import (
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	clientWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	accounting "github.com/nspcc-dev/neofs-node/pkg/network/transport/accounting/grpc"
	"github.com/pkg/errors"
	"go.uber.org/dig"
)

type balanceContractResult struct {
	dig.Out

	Client *clientWrapper.Wrapper

	AccountingService accounting.Service
}

// BalanceContractName is a name of Balance contract config sub-section.
const BalanceContractName = "balance"

const (
	balanceContractBalanceOfOpt = "balance_of_method"

	balanceContractDecimalsOfOpt = "decimals_method"
)

// BalanceContractBalanceOfOptPath is a path to balanceOf method name option.
func BalanceContractBalanceOfOptPath() string {
	return optPath(prefix, BalanceContractName, balanceContractBalanceOfOpt)
}

// BalanceContractDecimalsOfOptPath is a path to decimals method name option.
func BalanceContractDecimalsOfOptPath() string {
	return optPath(prefix, BalanceContractName, balanceContractDecimalsOfOpt)
}

func newBalanceContract(p contractParams) (res balanceContractResult, err error) {
	client, ok := p.MorphContracts[BalanceContractName]
	if !ok {
		err = errors.Errorf("missing %s contract client", BalanceContractName)
		return
	}

	var (
		balanceOfMethod = p.Viper.GetString(BalanceContractBalanceOfOptPath())
		decimalsMethod  = p.Viper.GetString(BalanceContractDecimalsOfOptPath())
	)

	var c *contract.Client
	if c, err = contract.New(client,
		contract.WithBalanceOfMethod(balanceOfMethod),
		contract.WithDecimalsMethod(decimalsMethod),
	); err != nil {
		return
	}

	if res.Client, err = clientWrapper.New(c); err != nil {
		return
	}

	if res.AccountingService, err = accounting.New(accounting.Params{
		ContractClient: res.Client,
	}); err != nil {
		return
	}

	return
}
