package morph

import (
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/services/public/accounting"
	"github.com/pkg/errors"
	"go.uber.org/dig"
)

type balanceContractResult struct {
	dig.Out

	BalanceContract implementations.MorphBalanceContract

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

	morphClient := implementations.MorphBalanceContract{}
	morphClient.SetBalanceContractClient(client)

	morphClient.SetBalanceOfMethodName(
		p.Viper.GetString(
			BalanceContractBalanceOfOptPath(),
		),
	)
	morphClient.SetDecimalsMethodName(
		p.Viper.GetString(
			BalanceContractDecimalsOfOptPath(),
		),
	)

	if res.AccountingService, err = accounting.New(accounting.Params{
		MorphBalanceContract: morphClient,
	}); err != nil {
		return
	}

	res.BalanceContract = morphClient

	return
}
