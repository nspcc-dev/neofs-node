package session

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/pkg/errors"
)

type ServiceExecutor interface {
	Create(context.Context, *session.CreateRequestBody) (*session.CreateResponseBody, error)
}

type executorSvc struct {
	exec ServiceExecutor
}

// NewExecutionService wraps ServiceExecutor and returns Session Service interface.
func NewExecutionService(exec ServiceExecutor) session.Service {
	return &executorSvc{
		exec: exec,
	}
}

func (s *executorSvc) Create(ctx context.Context, req *session.CreateRequest) (*session.CreateResponse, error) {
	respBody, err := s.exec.Create(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Create request")
	}

	resp := new(session.CreateResponse)
	resp.SetBody(respBody)

	return resp, nil
}
