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
	stream *response.ServerMessageStreamer
}

type getStreamResponser struct {
	util.ServerStream

	respWriter util.ResponseMessageWriter
}

type getRangeStreamResponser struct {
	stream *response.ServerMessageStreamer
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
		respWriter: s.respSvc.HandleServerStreamRequest_(func(resp util.ResponseMessage) error {
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

func (s *ResponseService) Put(ctx context.Context) (object.PutObjectStreamer, error) {
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

func (s *searchStreamResponser) Recv() (*object.SearchResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive response", s)
	}

	return r.(*object.SearchResponse), nil
}

func (s *ResponseService) Search(ctx context.Context, req *object.SearchRequest) (object.SearchObjectStreamer, error) {
	stream, err := s.respSvc.HandleServerStreamRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessageReader, error) {
			stream, err := s.svc.Search(ctx, req.(*object.SearchRequest))
			if err != nil {
				return nil, err
			}

			return func() (util.ResponseMessage, error) {
				return stream.Recv()
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &searchStreamResponser{
		stream: stream,
	}, nil
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

func (s *getRangeStreamResponser) Recv() (*object.GetRangeResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive response", s)
	}

	return r.(*object.GetRangeResponse), nil
}

func (s *ResponseService) GetRange(ctx context.Context, req *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
	stream, err := s.respSvc.HandleServerStreamRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessageReader, error) {
			stream, err := s.svc.GetRange(ctx, req.(*object.GetRangeRequest))
			if err != nil {
				return nil, err
			}

			return func() (util.ResponseMessage, error) {
				return stream.Recv()
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &getRangeStreamResponser{
		stream: stream,
	}, nil
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
