package getsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

// Service implements Get operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *getsvc.Service

	keyStorage *objutil.KeyStorage
}

// NewService constructs Service instance from provided options.
func NewService(opts ...Option) *Service {
	c := new(cfg)

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

// Get calls internal service and returns v2 object stream.
func (s *Service) Get(req *objectV2.GetRequest, stream objectSvc.GetObjectStream) error {
	p, err := s.toPrm(req, stream)
	if err != nil {
		return err
	}

	err = s.svc.Get(stream.Context(), *p)

	var splitErr *object.SplitInfoError

	switch {
	case errors.As(err, &splitErr):
		return stream.Send(splitInfoResponse(splitErr.SplitInfo()))
	default:
		return err
	}
}

// GetRange calls internal service and returns v2 payload range stream.
func (s *Service) GetRange(req *objectV2.GetRangeRequest, stream objectSvc.GetObjectRangeStream) error {
	p, err := s.toRangePrm(req, stream)
	if err != nil {
		return err
	}

	err = s.svc.GetRange(stream.Context(), *p)

	var splitErr *object.SplitInfoError

	switch {
	case errors.As(err, &splitErr):
		return stream.Send(splitInfoRangeResponse(splitErr.SplitInfo()))
	default:
		return err
	}
}

// GetRangeHash calls internal service and returns v2 response.
func (s *Service) GetRangeHash(ctx context.Context, req *objectV2.GetRangeHashRequest) (*objectV2.GetRangeHashResponse, error) {
	p, err := s.toHashRangePrm(req)
	if err != nil {
		return nil, err
	}

	res, err := s.svc.GetRangeHash(ctx, *p)
	if err != nil {
		return nil, err
	}

	return toHashResponse(req.GetBody().GetType(), res), nil
}

// Head serves NeoFS API v2 compatible HEAD requests.
func (s *Service) Head(ctx context.Context, req *objectV2.HeadRequest) (*objectV2.HeadResponse, error) {
	resp := new(objectV2.HeadResponse)
	resp.SetBody(new(objectV2.HeadResponseBody))

	p, err := s.toHeadPrm(ctx, req, resp)
	if err != nil {
		return nil, err
	}

	err = s.svc.Head(ctx, *p)

	var splitErr *object.SplitInfoError

	if errors.As(err, &splitErr) {
		setSplitInfoHeadResponse(splitErr.SplitInfo(), resp)
		err = nil
	}

	return resp, err
}

func WithInternalService(v *getsvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}

// WithKeyStorage returns option to set local private key storage.
func WithKeyStorage(ks *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = ks
	}
}
