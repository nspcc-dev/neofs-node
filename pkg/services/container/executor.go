package container

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type ServiceExecutor interface {
	Put(context.Context, *session.Token, *container.PutRequestBody) (*container.PutResponseBody, error)
	Delete(context.Context, *session.Token, *container.DeleteRequestBody) (*container.DeleteResponseBody, error)
	Get(context.Context, *container.GetRequestBody) (*container.GetResponseBody, error)
	List(context.Context, *container.ListRequestBody) (*container.ListResponseBody, error)
	SetExtendedACL(context.Context, *session.Token, *container.SetExtendedACLRequestBody) (*container.SetExtendedACLResponseBody, error)
	GetExtendedACL(context.Context, *container.GetExtendedACLRequestBody) (*container.GetExtendedACLResponseBody, error)
}

type executorSvc struct {
	Server

	exec ServiceExecutor
}

// NewExecutionService wraps ServiceExecutor and returns Container Service interface.
func NewExecutionService(exec ServiceExecutor) Server {
	return &executorSvc{
		exec: exec,
	}
}

func (s *executorSvc) Put(ctx context.Context, req *container.PutRequest) (*container.PutResponse, error) {
	meta := req.GetMetaHeader()
	for origin := meta.GetOrigin(); origin != nil; origin = meta.GetOrigin() {
		meta = origin
	}

	respBody, err := s.exec.Put(ctx, meta.GetSessionToken(), req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute Put request: %w", err)
	}

	resp := new(container.PutResponse)
	resp.SetBody(respBody)

	return resp, nil
}

func (s *executorSvc) Delete(ctx context.Context, req *container.DeleteRequest) (*container.DeleteResponse, error) {
	meta := req.GetMetaHeader()
	for origin := meta.GetOrigin(); origin != nil; origin = meta.GetOrigin() {
		meta = origin
	}

	respBody, err := s.exec.Delete(ctx, meta.GetSessionToken(), req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute Delete request: %w", err)
	}

	resp := new(container.DeleteResponse)
	resp.SetBody(respBody)

	return resp, nil
}

func (s *executorSvc) Get(ctx context.Context, req *container.GetRequest) (*container.GetResponse, error) {
	respBody, err := s.exec.Get(ctx, req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute Get request: %w", err)
	}

	resp := new(container.GetResponse)
	resp.SetBody(respBody)

	return resp, nil
}

func (s *executorSvc) List(ctx context.Context, req *container.ListRequest) (*container.ListResponse, error) {
	respBody, err := s.exec.List(ctx, req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute List request: %w", err)
	}

	resp := new(container.ListResponse)
	resp.SetBody(respBody)

	return resp, nil
}

func (s *executorSvc) SetExtendedACL(ctx context.Context, req *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	meta := req.GetMetaHeader()
	for origin := meta.GetOrigin(); origin != nil; origin = meta.GetOrigin() {
		meta = origin
	}

	respBody, err := s.exec.SetExtendedACL(ctx, meta.GetSessionToken(), req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute SetEACL request: %w", err)
	}

	resp := new(container.SetExtendedACLResponse)
	resp.SetBody(respBody)

	return resp, nil
}

func (s *executorSvc) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	respBody, err := s.exec.GetExtendedACL(ctx, req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute GetEACL request: %w", err)
	}

	resp := new(container.GetExtendedACLResponse)
	resp.SetBody(respBody)

	return resp, nil
}
