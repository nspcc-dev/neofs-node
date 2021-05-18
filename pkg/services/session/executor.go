package session

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type ServiceExecutor interface {
	Create(context.Context, *session.CreateRequestBody) (*session.CreateResponseBody, error)
}

type executorSvc struct {
	exec ServiceExecutor
}

// NewExecutionService wraps ServiceExecutor and returns Session Service interface.
func NewExecutionService(exec ServiceExecutor) Server {
	return &executorSvc{
		exec: exec,
	}
}

func (s *executorSvc) Create(ctx context.Context, req *session.CreateRequest) (*session.CreateResponse, error) {
	respBody, err := s.exec.Create(ctx, req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute Create request: %w", err)
	}

	resp := new(session.CreateResponse)
	resp.SetBody(respBody)

	return resp, nil
}
