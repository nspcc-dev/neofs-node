package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/pkg/errors"
)

type ServiceExecutor interface {
	Balance(context.Context, *accounting.BalanceRequestBody) (*accounting.BalanceResponseBody, error)
}

type executorSvc struct {
	exec ServiceExecutor

	metaHeader *session.ResponseMetaHeader
}

// NewExecutionService wraps ServiceExecutor and returns Accounting Service interface.
//
// Passed meta header is attached to all responses.
func NewExecutionService(exec ServiceExecutor, metaHdr *session.ResponseMetaHeader) accounting.Service {
	return &executorSvc{
		exec:       exec,
		metaHeader: metaHdr,
	}
}

func (s *executorSvc) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	respBody, err := s.exec.Balance(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Balance request")
	}

	resp := new(accounting.BalanceResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}
