package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
	"github.com/pkg/errors"
)

type responseService struct {
	respSvc *response.Service

	svc object.Service
}

type searchStreamResponser struct {
	stream *response.ServerMessageStreamer
}

type getStreamResponser struct {
	stream *response.ServerMessageStreamer
}

type getRangeStreamResponser struct {
	stream *response.ServerMessageStreamer
}

type putStreamResponser struct {
	stream *response.ClientMessageStreamer
}

// NewResponseService returns object service instance that passes internal service
// call to response service.
func NewResponseService(objSvc object.Service, respSvc *response.Service) object.Service {
	return &responseService{
		respSvc: respSvc,
		svc:     objSvc,
	}
}

func (s *getStreamResponser) Recv() (*object.GetResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive response", s)
	}

	return r.(*object.GetResponse), nil
}

func (s *responseService) Get(ctx context.Context, req *object.GetRequest) (object.GetObjectStreamer, error) {
	stream, err := s.respSvc.HandleServerStreamRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessageReader, error) {
			stream, err := s.svc.Get(ctx, req.(*object.GetRequest))
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

	return &getStreamResponser{
		stream: stream,
	}, nil
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

func (s *responseService) Put(ctx context.Context) (object.PutObjectStreamer, error) {
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

func (s *responseService) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
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

func (s *responseService) Search(ctx context.Context, req *object.SearchRequest) (object.SearchObjectStreamer, error) {
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

func (s *responseService) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
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

func (s *responseService) GetRange(ctx context.Context, req *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
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

func (s *responseService) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
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
