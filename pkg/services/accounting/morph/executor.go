package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	accountingSvc "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
)

type morphExecutor struct {
	client *wrapper.Wrapper
}

func NewExecutor(client *wrapper.Wrapper) accountingSvc.ServiceExecutor {
	return &morphExecutor{
		client: client,
	}
}

func (s *morphExecutor) Balance(ctx context.Context, body *accounting.BalanceRequestBody) (*accounting.BalanceResponseBody, error) {
	amount, err := s.client.BalanceOf(owner.NewIDFromV2(body.GetOwnerID()))
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
