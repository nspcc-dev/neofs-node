package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	accountingSvc "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
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

	precision, err := s.client.Decimals()
	if err != nil {
		return nil, err
	}

	dec := new(accounting.Decimal)
	dec.SetValue(amount)
	dec.SetPrecision(precision)

	res := new(accounting.BalanceResponseBody)
	res.SetBalance(dec)

	return res, nil
}
