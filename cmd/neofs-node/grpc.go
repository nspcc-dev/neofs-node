package main

import (
	"context"
	"fmt"
	"io"
	"net"

	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type containerSvc struct{}

type objectSvc struct{}

func unimplementedErr(srv, call string) error {
	return errors.Errorf("unimplemented API service call %s.%s", srv, call)
}

func (s *containerSvc) Put(context.Context, *containerGRPC.PutRequest) (*containerGRPC.PutResponse, error) {
	return nil, unimplementedErr("Container", "Put")
}

func (s *containerSvc) Delete(context.Context, *containerGRPC.DeleteRequest) (*containerGRPC.DeleteResponse, error) {
	return nil, unimplementedErr("Container", "Delete")
}

func (s *containerSvc) Get(context.Context, *containerGRPC.GetRequest) (*containerGRPC.GetResponse, error) {
	return nil, unimplementedErr("Container", "Get")
}

func (s *containerSvc) List(context.Context, *containerGRPC.ListRequest) (*containerGRPC.ListResponse, error) {
	return nil, unimplementedErr("Container", "List")
}

func (s *containerSvc) SetExtendedACL(context.Context, *containerGRPC.SetExtendedACLRequest) (*containerGRPC.SetExtendedACLResponse, error) {
	return nil, unimplementedErr("Container", "SetExtendedACL")
}

func (s *containerSvc) GetExtendedACL(context.Context, *containerGRPC.GetExtendedACLRequest) (*containerGRPC.GetExtendedACLResponse, error) {
	return nil, unimplementedErr("Container", "GetExtendedACL")
}

func (s *objectSvc) Get(context.Context, *object.GetRequest) (object.GetObjectStreamer, error) {
	return nil, unimplementedErr("Object", "Get")
}

func (s *objectSvc) Put(context.Context) (object.PutObjectStreamer, error) {
	return nil, unimplementedErr("Object", "Put")
}

func (s *objectSvc) Head(context.Context, *object.HeadRequest) (*object.HeadResponse, error) {
	return nil, unimplementedErr("Object", "Put")
}

type simpleSearchStreamer struct {
	count int
}

func (s *simpleSearchStreamer) Recv() (*object.SearchResponse, error) {
	resp := new(object.SearchResponse)

	body := new(object.SearchResponseBody)
	resp.SetBody(body)

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

	return resp, nil
}

func (s *objectSvc) Search(context.Context, *object.SearchRequest) (object.SearchObjectStreamer, error) {
	return new(simpleSearchStreamer), nil
}

func (s *objectSvc) Delete(context.Context, *object.DeleteRequest) (*object.DeleteResponse, error) {
	return nil, unimplementedErr("Object", "Delete")
}

func (s *objectSvc) GetRange(context.Context, *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
	return nil, unimplementedErr("Object", "GetRange")
}

func (s *objectSvc) GetRangeHash(context.Context, *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	return nil, unimplementedErr("Object", "GetRangeHash")
}

func initGRPC(c *cfg) {
	var err error

	c.cfgGRPC.listener, err = net.Listen("tcp", c.cfgGRPC.endpoint)
	fatalOnErr(err)

	c.cfgGRPC.server = grpc.NewServer()
}

func serveGRPC(c *cfg) {
	go func() {
		c.wg.Add(1)
		defer func() {
			c.wg.Done()
		}()

		if err := c.cfgGRPC.server.Serve(c.cfgGRPC.listener); err != nil {
			fmt.Println("gRPC server error", err)
		}
	}()
}
