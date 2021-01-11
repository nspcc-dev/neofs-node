package object

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/pkg/errors"
)

type SignService struct {
	key *ecdsa.PrivateKey

	sigSvc *util.SignService

	svc ServiceServer
}

type searchStreamSigner struct {
	util.ServerStream

	respWriter util.ResponseMessageWriter
}

type getStreamSigner struct {
	util.ServerStream

	respWriter util.ResponseMessageWriter
}

type putStreamSigner struct {
	stream *util.RequestMessageStreamer
}

type getRangeStreamSigner struct {
	util.ServerStream

	respWriter util.ResponseMessageWriter
}

func NewSignService(key *ecdsa.PrivateKey, svc ServiceServer) *SignService {
	return &SignService{
		key:    key,
		sigSvc: util.NewUnarySignService(key),
		svc:    svc,
	}
}

func (s *getStreamSigner) Send(resp *object.GetResponse) error {
	return s.respWriter(resp)
}

func (s *SignService) Get(req *object.GetRequest, stream GetObjectStream) error {
	respWriter, err := s.sigSvc.HandleServerStreamRequest(req,
		func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.GetResponse))
		},
	)
	if err != nil {
		return err
	}

	return s.svc.Get(req, &getStreamSigner{
		ServerStream: stream,
		respWriter:   respWriter,
	})
}

func (s *putStreamSigner) Send(req *object.PutRequest) error {
	return s.stream.Send(req)
}

func (s *putStreamSigner) CloseAndRecv() (*object.PutResponse, error) {
	r, err := s.stream.CloseAndRecv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response")
	}

	return r.(*object.PutResponse), nil
}

func (s *SignService) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	stream, err := s.svc.Put(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not create Put object streamer")
	}

	return &putStreamSigner{
		stream: s.sigSvc.CreateRequestStreamer(
			func(req interface{}) error {
				return stream.Send(req.(*object.PutRequest))
			},
			func() (util.ResponseMessage, error) {
				return stream.CloseAndRecv()
			},
		),
	}, nil
}

func (s *SignService) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Head(ctx, req.(*object.HeadRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.HeadResponse), nil
}

func (s *searchStreamSigner) Send(resp *object.SearchResponse) error {
	return s.respWriter(resp)
}

func (s *SignService) Search(req *object.SearchRequest, stream SearchStream) error {
	respWriter, err := s.sigSvc.HandleServerStreamRequest(req,
		func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.SearchResponse))
		},
	)
	if err != nil {
		return err
	}

	return s.svc.Search(req, &searchStreamSigner{
		ServerStream: stream,
		respWriter:   respWriter,
	})
}

func (s *SignService) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Delete(ctx, req.(*object.DeleteRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.DeleteResponse), nil
}

func (s *getRangeStreamSigner) Send(resp *object.GetRangeResponse) error {
	return s.respWriter(resp)
}

func (s *SignService) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	respWriter, err := s.sigSvc.HandleServerStreamRequest(req,
		func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.GetRangeResponse))
		},
	)
	if err != nil {
		return err
	}

	return s.svc.GetRange(req, &getRangeStreamSigner{
		ServerStream: stream,
		respWriter:   respWriter,
	})
}

func (s *SignService) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.GetRangeHash(ctx, req.(*object.GetRangeHashRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.GetRangeHashResponse), nil
}
