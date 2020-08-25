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
}

type searchStreamSigner struct {
	key *ecdsa.PrivateKey

	stream object.SearchObjectStreamer
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
	}
}

func (s *signService) Get(context.Context, *object.GetRequest) (object.GetObjectStreamer, error) {
	panic("implement me")
}

func (s *signService) Put(context.Context) (object.PutObjectStreamer, error) {
	panic("implement me")
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
