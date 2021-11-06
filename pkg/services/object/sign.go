package object

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type SignService struct {
	key *ecdsa.PrivateKey

	sigSvc *util.SignService

	svc ServiceServer
}

type searchStreamSigner struct {
	util.ServerStream

	respWriter util.ResponseMessageWriter

	nonEmptyResp bool // set on first Send call
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
	return s.sigSvc.HandleServerStreamRequest(req,
		func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.GetResponse))
		},
		func() util.ResponseMessage {
			return new(object.GetResponse)
		},
		func(respWriter util.ResponseMessageWriter) error {
			return s.svc.Get(req, &getStreamSigner{
				ServerStream: stream,
				respWriter:   respWriter,
			})
		},
	)
}

func (s *putStreamSigner) Send(req *object.PutRequest) error {
	return s.stream.Send(req)
}

func (s *putStreamSigner) CloseAndRecv() (*object.PutResponse, error) {
	r, err := s.stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("could not receive response: %w", err)
	}

	return r.(*object.PutResponse), nil
}

func (s *SignService) Put(ctx context.Context) (PutObjectStream, error) {
	stream, err := s.svc.Put(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not create Put object streamer: %w", err)
	}

	return &putStreamSigner{
		stream: s.sigSvc.CreateRequestStreamer(
			func(req interface{}) error {
				return stream.Send(req.(*object.PutRequest))
			},
			func() (util.ResponseMessage, error) {
				return stream.CloseAndRecv()
			},
			func() util.ResponseMessage {
				return new(object.PutResponse)
			},
		),
	}, nil
}

func (s *SignService) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Head(ctx, req.(*object.HeadRequest))
		},
		func() util.ResponseMessage {
			return new(object.HeadResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.HeadResponse), nil
}

func (s *searchStreamSigner) Send(resp *object.SearchResponse) error {
	s.nonEmptyResp = true
	return s.respWriter(resp)
}

func (s *SignService) Search(req *object.SearchRequest, stream SearchStream) error {
	return s.sigSvc.HandleServerStreamRequest(req,
		func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.SearchResponse))
		},
		func() util.ResponseMessage {
			return new(object.SearchResponse)
		},
		func(respWriter util.ResponseMessageWriter) error {
			stream := &searchStreamSigner{
				ServerStream: stream,
				respWriter:   respWriter,
			}

			err := s.svc.Search(req, stream)

			if err == nil && !stream.nonEmptyResp {
				// The higher component does not write any response in the case of an empty result (which is correct).
				// With the introduction of status returns at least one answer must be signed and sent to the client.
				// This approach is supported by clients who do not know how to work with statuses (one could make
				// a switch according to the protocol version from the request, but the costs of sending an empty
				// answer can be neglected due to the gradual refusal to use the "old" clients).
				return stream.Send(new(object.SearchResponse))
			}

			return nil
		},
	)
}

func (s *SignService) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Delete(ctx, req.(*object.DeleteRequest))
		},
		func() util.ResponseMessage {
			return new(object.DeleteResponse)
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
	return s.sigSvc.HandleServerStreamRequest(req,
		func(resp util.ResponseMessage) error {
			return stream.Send(resp.(*object.GetRangeResponse))
		},
		func() util.ResponseMessage {
			return new(object.GetRangeResponse)
		},
		func(respWriter util.ResponseMessageWriter) error {
			return s.svc.GetRange(req, &getRangeStreamSigner{
				ServerStream: stream,
				respWriter:   respWriter,
			})
		},
	)
}

func (s *SignService) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.GetRangeHash(ctx, req.(*object.GetRangeHashRequest))
		},
		func() util.ResponseMessage {
			return new(object.GetRangeHashResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*object.GetRangeHashResponse), nil
}
