package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/pkg/errors"
)

type GetObjectBodyStreamer interface {
	Recv() (*object.GetResponseBody, error)
}

type PutObjectBodyStreamer interface {
	Send(*object.PutRequestBody) error
	CloseAndRecv() (*object.PutResponseBody, error)
}

type SearchObjectBodyStreamer interface {
	Recv() (*object.SearchResponseBody, error)
}

type GetRangeObjectBodyStreamer interface {
	Recv() (*object.GetRangeResponseBody, error)
}

type ServiceExecutor interface {
	Get(context.Context, *object.GetRequestBody) (GetObjectBodyStreamer, error)
	Put(context.Context) (PutObjectBodyStreamer, error)
	Head(context.Context, *object.HeadRequestBody) (*object.HeadResponseBody, error)
	Search(context.Context, *object.SearchRequestBody) (SearchObjectBodyStreamer, error)
	Delete(context.Context, *object.DeleteRequestBody) (*object.DeleteResponseBody, error)
	GetRange(context.Context, *object.GetRangeRequestBody) (GetRangeObjectBodyStreamer, error)
	GetRangeHash(context.Context, *object.GetRangeHashRequestBody) (*object.GetRangeHashResponseBody, error)
}

type executorSvc struct {
	exec ServiceExecutor

	metaHeader *session.ResponseMetaHeader
}

type searchStreamer struct {
	bodyStreamer SearchObjectBodyStreamer

	metaHdr *session.ResponseMetaHeader
}

type getStreamer struct {
	bodyStreamer GetObjectBodyStreamer

	metaHdr *session.ResponseMetaHeader
}

type putStreamer struct {
	bodyStreamer PutObjectBodyStreamer

	metaHdr *session.ResponseMetaHeader
}

type rangeStreamer struct {
	bodyStreamer GetRangeObjectBodyStreamer

	metaHdr *session.ResponseMetaHeader
}

// NewExecutionService wraps ServiceExecutor and returns Object Service interface.
//
// Passed meta header is attached to all responses.
func NewExecutionService(exec ServiceExecutor, metaHdr *session.ResponseMetaHeader) object.Service {
	return &executorSvc{
		exec:       exec,
		metaHeader: metaHdr,
	}
}

func (s *getStreamer) Recv() (*object.GetResponse, error) {
	body, err := s.bodyStreamer.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response body")
	}

	resp := new(object.GetResponse)
	resp.SetBody(body)
	resp.SetMetaHeader(s.metaHdr)

	return resp, nil
}

func (s *executorSvc) Get(ctx context.Context, req *object.GetRequest) (object.GetObjectStreamer, error) {
	bodyStream, err := s.exec.Get(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Get request")
	}

	return &getStreamer{
		bodyStreamer: bodyStream,
		metaHdr:      s.metaHeader,
	}, nil
}

func (s *putStreamer) Send(req *object.PutRequest) error {
	return s.bodyStreamer.Send(req.GetBody())
}

func (s *putStreamer) CloseAndRecv() (*object.PutResponse, error) {
	body, err := s.bodyStreamer.CloseAndRecv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response body")
	}

	resp := new(object.PutResponse)
	resp.SetBody(body)
	resp.SetMetaHeader(s.metaHdr)

	return resp, nil
}

func (s *executorSvc) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	bodyStream, err := s.exec.Put(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Put request")
	}

	return &putStreamer{
		bodyStreamer: bodyStream,
		metaHdr:      s.metaHeader,
	}, nil
}

func (s *executorSvc) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	respBody, err := s.exec.Head(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Head request")
	}

	resp := new(object.HeadResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}

func (s *searchStreamer) Recv() (*object.SearchResponse, error) {
	body, err := s.bodyStreamer.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response body")
	}

	resp := new(object.SearchResponse)
	resp.SetBody(body)
	resp.SetMetaHeader(s.metaHdr)

	return resp, nil
}

func (s *executorSvc) Search(ctx context.Context, req *object.SearchRequest) (object.SearchObjectStreamer, error) {
	bodyStream, err := s.exec.Search(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Search request")
	}

	return &searchStreamer{
		bodyStreamer: bodyStream,
		metaHdr:      s.metaHeader,
	}, nil
}

func (s *executorSvc) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	respBody, err := s.exec.Delete(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute Delete request")
	}

	resp := new(object.DeleteResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}

func (s *rangeStreamer) Recv() (*object.GetRangeResponse, error) {
	body, err := s.bodyStreamer.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response body")
	}

	resp := new(object.GetRangeResponse)
	resp.SetBody(body)
	resp.SetMetaHeader(s.metaHdr)

	return resp, nil
}

func (s *executorSvc) GetRange(ctx context.Context, req *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
	bodyStream, err := s.exec.GetRange(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute GetRange request")
	}

	return &rangeStreamer{
		bodyStreamer: bodyStream,
		metaHdr:      s.metaHeader,
	}, nil
}

func (s *executorSvc) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	respBody, err := s.exec.GetRangeHash(ctx, req.GetBody())
	if err != nil {
		return nil, errors.Wrap(err, "could not execute GetRangeHash request")
	}

	resp := new(object.GetRangeHashResponse)
	resp.SetBody(respBody)
	resp.SetMetaHeader(s.metaHeader)

	return resp, nil
}
