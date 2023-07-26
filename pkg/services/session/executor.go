package session

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"go.uber.org/zap"
)

type ServiceExecutor interface {
	Create(context.Context, *session.CreateRequestBody) (*session.CreateResponseBody, error)
}

type executorSvc struct {
	exec ServiceExecutor

	log *zap.Logger
}

// NewExecutionService wraps ServiceExecutor and returns Session Service interface.
func NewExecutionService(exec ServiceExecutor, l *zap.Logger) Server {
	return &executorSvc{
		exec: exec,
		log:  l,
	}
}

func (s *executorSvc) Create(ctx context.Context, req *session.CreateRequest) (*session.CreateResponse, error) {
	s.log.Debug("serving request...",
		zap.String("component", "SessionService"),
		zap.String("request", "Create"),
	)

	respBody, err := s.exec.Create(ctx, req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute Create request: %w", err)
	}

	resp := new(session.CreateResponse)
	resp.SetBody(respBody)

	return resp, nil
}
