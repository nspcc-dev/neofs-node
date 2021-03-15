package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
	"github.com/pkg/errors"
)

type ResponseService struct {
	respSvc *response.Service

	svc ServiceServer
}

type searchStreamResponser struct {
	util.ServerStream

	respWriter util.ResponseMessageWriter
}

type getStreamResponser struct {
	util.ServerStream

	respWriter util.ResponseMessageWriter
}

type getRangeStreamResponser struct {
	util.ServerStream

	respWriter util.ResponseMessageWriter
}

type putStreamResponser struct {
	stream *response.ClientMessageStreamer
}

// NewResponseService returns object service instance that passes internal service
// call to response service.
func NewResponseService(objSvc ServiceServer, respSvc *response.Service) *ResponseService {
	return &ResponseService{
		respSvc: respSvc,
		svc:     objSvc,
	}
}

func (s *getStreamResponser) Send(resp *object.GetResponse) error {
	return s.respWriter(resp)
}

func (s *ResponseService) Get(req *object.GetRequest, stream GetObjectStream) error {
	return s.svc.Get(req, &getStreamResponser{
		ServerStream: stream,
		respWriter: s.respSvc.HandleServerStreamRequest(func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.GetResponse))
		}),
	})
}

func (s *putStreamResponser) Send(req *object.PutRequest) error {
	return s.stream.Send(req)
}

func (s *putStreamResponser) CloseAndRecv() (*object.PutResponse, error) {
	r, err := s.stream.CloseAndRecv()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive response", s)
	}

	return r.(*object.PutResponse), nil
}

func (s *ResponseService) Put(ctx context.Context) (PutObjectStream, error) {
	stream, err := s.svc.Put(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not create Put object streamer")
	}

	return &putStreamResponser{
		stream: s.respSvc.CreateRequestStreamer(
			func(req interface{}) error {
				return stream.Send(req.(*object.PutRequest))
			},
			func() (util.ResponseMessage, error) {
				return stream.CloseAndRecv()
			},
		),
	}, nil
}

func (s *ResponseService) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Head(ctx, req.(*object.HeadRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.HeadResponse), nil
}

func (s *searchStreamResponser) Send(resp *object.SearchResponse) error {
	return s.respWriter(resp)
}

func (s *ResponseService) Search(req *object.SearchRequest, stream SearchStream) error {
	return s.svc.Search(req, &searchStreamResponser{
		ServerStream: stream,
		respWriter: s.respSvc.HandleServerStreamRequest(func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.SearchResponse))
		}),
	})
}

func (s *ResponseService) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Delete(ctx, req.(*object.DeleteRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.DeleteResponse), nil
}

func (s *getRangeStreamResponser) Send(resp *object.GetRangeResponse) error {
	return s.respWriter(resp)
}

func (s *ResponseService) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	return s.svc.GetRange(req, &getRangeStreamResponser{
		ServerStream: stream,
		respWriter: s.respSvc.HandleServerStreamRequest(func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.GetRangeResponse))
		}),
	})
}

func (s *ResponseService) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.GetRangeHash(ctx, req.(*object.GetRangeHashRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.GetRangeHashResponse), nil
}
