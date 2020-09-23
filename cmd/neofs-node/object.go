package main

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
)

type objectExecutor struct{}

func (*objectExecutor) Get(context.Context, *object.GetRequestBody) (objectService.GetObjectBodyStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) Put(context.Context) (objectService.PutObjectBodyStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) Head(context.Context, *object.HeadRequestBody) (*object.HeadResponseBody, error) {
	return nil, errors.New("unimplemented service call")
}

func (s *objectExecutor) Search(context.Context, *object.SearchRequestBody) (objectService.SearchObjectBodyStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) Delete(_ context.Context, body *object.DeleteRequestBody) (*object.DeleteResponseBody, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) GetRange(_ context.Context, body *object.GetRangeRequestBody) (objectService.GetRangeObjectBodyStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) GetRangeHash(context.Context, *object.GetRangeHashRequestBody) (*object.GetRangeHashResponseBody, error) {
	return nil, errors.New("unimplemented service call")
}

func initObjectService(c *cfg) {
	metaHdr := new(session.ResponseMetaHeader)
	xHdr := new(session.XHeader)
	xHdr.SetKey("test X-Header key for Object service")
	xHdr.SetValue("test X-Header value for Object service")
	metaHdr.SetXHeaders([]*session.XHeader{xHdr})

	objectGRPC.RegisterObjectServiceServer(c.cfgGRPC.server,
		objectTransportGRPC.New(
			objectService.NewSignService(
				c.key,
				objectService.NewExecutionService(
					new(objectExecutor),
					metaHdr,
				),
			),
		),
	)
}
