package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	accountingSvc "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
)

type morphExecutor struct {
	client *wrapper.Wrapper
}

const fixed8Precision = 8

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

	// Convert amount to Fixed8 precision. This way it will definitely fit
	// int64 value.
	// Max Fixed8 decimal integer value that fit into int64: 92 233 720 368.
	// Max Fixed12 decimal integer value that fit into int64: 9 223 372.
	// Max Fixed16 decimal integer value that fit into int64: 922.
	fixed8Amount := precision.Convert(balancePrecision, fixed8Precision, amount)

	dec := new(accounting.Decimal)
	dec.SetValue(fixed8Amount.Int64())
	dec.SetPrecision(fixed8Precision)

	res := new(accounting.BalanceResponseBody)
	res.SetBalance(dec)

	return res, nil
}
