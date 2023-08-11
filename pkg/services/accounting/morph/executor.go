package accounting

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	accountingSvc "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type morphExecutor struct {
	client *balance.Client
}

func NewExecutor(client *balance.Client) accountingSvc.ServiceExecutor {
	return &morphExecutor{
		client: client,
	}
}

func (s *morphExecutor) Balance(_ context.Context, body *accounting.BalanceRequestBody) (*accounting.BalanceResponseBody, error) {
	idV2 := body.GetOwnerID()
	if idV2 == nil {
		return nil, errors.New("missing account")
	}

	var id user.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, fmt.Errorf("invalid account: %w", err)
	}

	amount, err := s.client.BalanceOf(id)
	if err != nil {
		return nil, err
	}

	balancePrecision, err := s.client.Decimals()
	if err != nil {
		return nil, err
	}

	dec := new(accounting.Decimal)
	dec.SetValue(amount.Int64())
	dec.SetPrecision(balancePrecision)

	res := new(accounting.BalanceResponseBody)
	res.SetBalance(dec)

	return res, nil
}
