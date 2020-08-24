package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/pkg/errors"
)

type ServiceExecutor interface {
	Put(context.Context, *container.PutRequestBody) (*container.PutResponseBody, error)
	Delete(context.Context, *container.DeleteRequestBody) (*container.DeleteResponseBody, error)
	Get(context.Context, *container.GetRequestBody) (*container.GetResponseBody, error)
	List(context.Context, *container.ListRequestBody) (*container.ListResponseBody, error)
	SetExtendedACL(context.Context, *container.SetExtendedACLRequestBody) (*container.SetExtendedACLResponseBody, error)
	GetExtendedACL(context.Context, *container.GetExtendedACLRequestBody) (*container.GetExtendedACLResponseBody, error)
}

type executorSvc struct {
	exec ServiceExecutor

	metaHeader *session.ResponseMetaHeader
}

// NewExecutionService wraps ServiceExecutor and returns Container Service interface.
//
// Passed meta header is attached to all responses.
func NewExecutionService(exec ServiceExecutor, metaHdr *session.ResponseMetaHeader) container.Service {
	return &executorSvc{
		exec:       exec,
		metaHeader: metaHdr,
	}
}

func (s *executorSvc) Put(ctx context.Context, req *container.PutRequest) (*container.PutResponse, error) {
	respBody, err := s.exec.Put(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Put request")
	}

	resp := new(container.PutResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}

func (s *executorSvc) Delete(ctx context.Context, req *container.DeleteRequest) (*container.DeleteResponse, error) {
	respBody, err := s.exec.Delete(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Delete request")
	}

	resp := new(container.DeleteResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}

func (s *executorSvc) Get(ctx context.Context, req *container.GetRequest) (*container.GetResponse, error) {
	respBody, err := s.exec.Get(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Get request")
	}

	resp := new(container.GetResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}

func (s *executorSvc) List(ctx context.Context, req *container.ListRequest) (*container.ListResponse, error) {
	respBody, err := s.exec.List(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute List request")
	}

	resp := new(container.ListResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}

func (s *executorSvc) SetExtendedACL(ctx context.Context, req *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	respBody, err := s.exec.SetExtendedACL(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute SetEACL request")
	}

	resp := new(container.SetExtendedACLResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}

func (s *executorSvc) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	respBody, err := s.exec.GetExtendedACL(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute GetEACL request")
	}

	resp := new(container.GetExtendedACLResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}
