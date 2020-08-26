package object

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/pkg/errors"
)

type signService struct {
	key *ecdsa.PrivateKey

	sigSvc *util.SignService

	svc object.Service
}

type searchStreamSigner struct {
	stream *util.MessageStreamer
}

type getStreamSigner struct {
	stream *util.MessageStreamer
}

type putStreamSigner struct {
	key *ecdsa.PrivateKey

	stream object.PutObjectStreamer
}

type getRangeStreamSigner struct {
	stream *util.MessageStreamer
}

func NewSignService(key *ecdsa.PrivateKey, svc object.Service) object.Service {
	return &signService{
		key:    key,
		sigSvc: util.NewUnarySignService(key),
		svc:    svc,
	}
}

func (s *getStreamSigner) Recv() (*object.GetResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response")
	}

	return r.(*object.GetResponse), nil
}

func (s *signService) Get(ctx context.Context, req *object.GetRequest) (object.GetObjectStreamer, error) {
	stream, err := s.sigSvc.HandleServerStreamRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.MessageReader, error) {
			stream, err := s.svc.Get(ctx, req.(*object.GetRequest))
			if err != nil {
				return nil, err
			}

			return func() (interface{}, error) {
				return stream.Recv()
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &getStreamSigner{
		stream: stream,
	}, nil
}

func (s *putStreamSigner) Send(req *object.PutRequest) error {
	if err := signature.VerifyServiceMessage(req); err != nil {
		return errors.Wrap(err, "could not verify request")
	}

	return s.stream.Send(req)
}

func (s *putStreamSigner) CloseAndRecv() (*object.PutResponse, error) {
	r, err := s.stream.CloseAndRecv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response")
	}

	if err := signature.SignServiceMessage(s.key, r); err != nil {
		return nil, errors.Wrap(err, "could not sign response")
	}

	return r, nil
}

func (s *signService) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	stream, err := s.svc.Put(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not create Put object streamer")
	}

	return &putStreamSigner{
		key:    s.key,
		stream: stream,
	}, nil
}

func (s *signService) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.Head(ctx, req.(*object.HeadRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.HeadResponse), nil
}

func (s *searchStreamSigner) Recv() (*object.SearchResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response")
	}

	return r.(*object.SearchResponse), nil
}

func (s *signService) Search(ctx context.Context, req *object.SearchRequest) (object.SearchObjectStreamer, error) {
	stream, err := s.sigSvc.HandleServerStreamRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.MessageReader, error) {
			stream, err := s.svc.Search(ctx, req.(*object.SearchRequest))
			if err != nil {
				return nil, err
			}

			return func() (interface{}, error) {
				return stream.Recv()
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &searchStreamSigner{
		stream: stream,
	}, nil
}

func (s *signService) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.Delete(ctx, req.(*object.DeleteRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.DeleteResponse), nil
}

func (s *getRangeStreamSigner) Recv() (*object.GetRangeResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response")
	}

	return r.(*object.GetRangeResponse), nil
}

func (s *signService) GetRange(ctx context.Context, req *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
	stream, err := s.sigSvc.HandleServerStreamRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.MessageReader, error) {
			stream, err := s.svc.GetRange(ctx, req.(*object.GetRangeRequest))
			if err != nil {
				return nil, err
			}

			return func() (interface{}, error) {
				return stream.Recv()
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &getRangeStreamSigner{
		stream: stream,
	}, nil
}

func (s *signService) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.GetRangeHash(ctx, req.(*object.GetRangeHashRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.GetRangeHashResponse), nil
}
