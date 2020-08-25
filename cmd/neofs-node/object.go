package main

import (
	"context"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
)

type simpleSearchBodyStreamer struct {
	count int
}

type simpleGetBodyStreamer struct {
	count int
}

type simplePutBodyStreamer struct{}

type simpleRangeBodyStreamer struct {
	count int
}

type objectExecutor struct{}

func (s *simpleGetBodyStreamer) Recv() (*object.GetResponseBody, error) {
	body := new(object.GetResponseBody)

	id := new(refs.ObjectID)
	id.SetValue([]byte{1, 2, 3})

	if s.count == 0 {
		in := new(object.GetObjectPartInit)
		in.SetObjectID(id)

		body.SetObjectPart(in)
	} else if s.count == 1 {
		c := new(object.GetObjectPartChunk)
		c.SetChunk([]byte{8, 8, 0, 0, 5, 5, 5, 3, 5, 3, 5})

		body.SetObjectPart(c)
	} else {
		return nil, io.EOF
	}

	s.count++

	return body, nil
}

func (*objectExecutor) Get(context.Context, *object.GetRequestBody) (objectService.GetObjectBodyStreamer, error) {
	return new(simpleGetBodyStreamer), nil
}

func (s *simplePutBodyStreamer) Send(body *object.PutRequestBody) error {
	fmt.Printf("object part received %T\n", body.GetObjectPart())
	return nil
}

func (s *simplePutBodyStreamer) CloseAndRecv() (*object.PutResponseBody, error) {
	body := new(object.PutResponseBody)
	oid := new(refs.ObjectID)

	body.SetObjectID(oid)

	oid.SetValue([]byte{6, 7, 8})

	return body, nil
}

func (*objectExecutor) Put(context.Context) (objectService.PutObjectBodyStreamer, error) {
	return new(simplePutBodyStreamer), nil
}

func (*objectExecutor) Head(_ context.Context, body *object.HeadRequestBody) (*object.HeadResponseBody, error) {
	res := new(object.HeadResponseBody)

	hdrPart := new(object.GetHeaderPartShort)
	shHdr := new(object.ShortHeader)
	hdrPart.SetShortHeader(shHdr)

	shHdr.SetPayloadLength(100)

	res.SetHeaderPart(hdrPart)

	return res, nil
}

func (s *objectExecutor) Search(ctx context.Context, body *object.SearchRequestBody) (objectService.SearchObjectBodyStreamer, error) {
	return new(simpleSearchBodyStreamer), nil
}

func (*objectExecutor) Delete(_ context.Context, body *object.DeleteRequestBody) (*object.DeleteResponseBody, error) {
	return new(object.DeleteResponseBody), nil
}

func (s *simpleRangeBodyStreamer) Recv() (*object.GetRangeResponseBody, error) {
	body := new(object.GetRangeResponseBody)

	if s.count == 0 {
		body.SetChunk([]byte{1, 2, 2, 1})
	} else if s.count == 1 {
		body.SetChunk([]byte{4, 2, 4, 2})
	} else {
		return nil, io.EOF
	}

	s.count++

	return body, nil
}

func (*objectExecutor) GetRange(_ context.Context, body *object.GetRangeRequestBody) (objectService.GetRangeObjectBodyStreamer, error) {
	return new(simpleRangeBodyStreamer), nil
}

func (*objectExecutor) GetRangeHash(context.Context, *object.GetRangeHashRequestBody) (*object.GetRangeHashResponseBody, error) {
	panic("implement me")
}

func (s *simpleSearchBodyStreamer) Recv() (*object.SearchResponseBody, error) {
	body := new(object.SearchResponseBody)

	id := new(refs.ObjectID)
	body.SetIDList([]*refs.ObjectID{id})

	if s.count == 0 {
		id.SetValue([]byte{1})
	} else if s.count == 1 {
		id.SetValue([]byte{2})
	} else {
		return nil, io.EOF
	}

	s.count++

	return body, nil
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
