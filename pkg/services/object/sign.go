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

	searchSigService *util.UnarySignService
	getSigService    *util.UnarySignService
	putService       object.Service
}

type searchStreamSigner struct {
	key *ecdsa.PrivateKey

	stream object.SearchObjectStreamer
}

type getStreamSigner struct {
	key *ecdsa.PrivateKey

	stream object.GetObjectStreamer
}

type putStreamSigner struct {
	key *ecdsa.PrivateKey

	stream object.PutObjectStreamer
}

func NewSignService(key *ecdsa.PrivateKey, svc object.Service) object.Service {
	return &signService{
		key: key,
		searchSigService: util.NewUnarySignService(
			nil, // private key is not needed because service returns stream
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.Search(ctx, req.(*object.SearchRequest))
			},
		),
		getSigService: util.NewUnarySignService(
			nil, // private key is not needed because service returns stream
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.Get(ctx, req.(*object.GetRequest))
			},
		),
		putService: svc,
	}
}

func (s *getStreamSigner) Recv() (*object.GetResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response")
	}

	if err := signature.SignServiceMessage(s.key, r); err != nil {
		return nil, errors.Wrap(err, "could not sign response")
	}

	return r, nil
}

func (s *signService) Get(ctx context.Context, req *object.GetRequest) (object.GetObjectStreamer, error) {
	resp, err := s.getSigService.HandleServerStreamRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return &getStreamSigner{
		key:    s.key,
		stream: resp.(object.GetObjectStreamer),
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
	stream, err := s.putService.Put(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not create Put object streamer")
	}

	return &putStreamSigner{
		key:    s.key,
		stream: stream,
	}, nil
}

func (s *signService) Head(context.Context, *object.HeadRequest) (*object.HeadResponse, error) {
	panic("implement me")
}

func (s *searchStreamSigner) Recv() (*object.SearchResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response")
	}

	if err := signature.SignServiceMessage(s.key, r); err != nil {
		return nil, errors.Wrap(err, "could not sign response")
	}

	return r, nil
}

func (s *signService) Search(ctx context.Context, req *object.SearchRequest) (object.SearchObjectStreamer, error) {
	resp, err := s.searchSigService.HandleServerStreamRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return &searchStreamSigner{
		key:    s.key,
		stream: resp.(object.SearchObjectStreamer),
	}, nil
}

func (s *signService) Delete(context.Context, *object.DeleteRequest) (*object.DeleteResponse, error) {
	panic("implement me")
}

func (s *signService) GetRange(context.Context, *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
	panic("implement me")
}

func (s *signService) GetRangeHash(context.Context, *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	panic("implement me")
}
