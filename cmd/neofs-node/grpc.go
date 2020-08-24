package main

import (
	"context"
	"fmt"
	"net"

	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object"
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

func (s *objectSvc) Get(context.Context, *objectGRPC.GetRequest) (objectGRPC.GetObjectStreamer, error) {
	return nil, unimplementedErr("Object", "Get")
}

func (s *objectSvc) Put(context.Context) (objectGRPC.PutObjectStreamer, error) {
	return nil, unimplementedErr("Object", "Put")
}

func (s *objectSvc) Head(context.Context, *objectGRPC.HeadRequest) (*objectGRPC.HeadResponse, error) {
	return nil, unimplementedErr("Object", "Put")
}

func (s *objectSvc) Search(context.Context, *objectGRPC.SearchRequest) (objectGRPC.SearchObjectStreamer, error) {
	return nil, unimplementedErr("Object", "Search")
}

func (s *objectSvc) Delete(context.Context, *objectGRPC.DeleteRequest) (*objectGRPC.DeleteResponse, error) {
	return nil, unimplementedErr("Object", "Delete")
}

func (s *objectSvc) GetRange(context.Context, *objectGRPC.GetRangeRequest) (objectGRPC.GetRangeObjectStreamer, error) {
	return nil, unimplementedErr("Object", "GetRange")
}

func (s *objectSvc) GetRangeHash(context.Context, *objectGRPC.GetRangeHashRequest) (*objectGRPC.GetRangeHashResponse, error) {
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
