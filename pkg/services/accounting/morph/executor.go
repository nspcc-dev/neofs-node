package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	accountingSvc "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
	"github.com/pkg/errors"
)

type morphExecutor struct {
	// TODO: use client wrapper
	client *balance.Client
}

func NewExecutor(client *balance.Client) accountingSvc.ServiceExecutor {
	return &morphExecutor{
		client: client,
	}
}

func (s *morphExecutor) Balance(ctx context.Context, body *accounting.BalanceRequestBody) (*accounting.BalanceResponseBody, error) {
	id := body.GetOwnerID()

	idBytes, err := id.StableMarshal(nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal wallet owner ID")
	}

	argsBalance := balance.GetBalanceOfArgs{}
	argsBalance.SetWallet(idBytes)

	vBalance, err := s.client.BalanceOf(argsBalance)
	if err != nil {
		return nil, errors.Wrap(err, "could not call BalanceOf method")
	}

	argsDecimals := balance.DecimalsArgs{}

	vDecimals, err := s.client.Decimals(argsDecimals)
	if err != nil {
		return nil, errors.Wrap(err, "could not call decimals method")
	}

	dec := new(accounting.Decimal)
	dec.SetValue(vBalance.Amount())
	dec.SetPrecision(uint32(vDecimals.Decimals()))

	res := new(accounting.BalanceResponseBody)
	res.SetBalance(dec)

	return res, nil
}
