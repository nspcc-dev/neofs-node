package main

import (
	"context"
	"fmt"
	"net"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container"
	container "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object"
	object "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	sessionGRPC "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	containerTransport "github.com/nspcc-dev/neofs-node/pkg/network/transport/container/grpc"
	objectTransport "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	sessionTransport "github.com/nspcc-dev/neofs-node/pkg/network/transport/session/grpc"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type accountingSvcExec struct{}

type sessionSvc struct{}

type containerSvc struct{}

type objectSvc struct{}

func unimplementedErr(srv, call string) error {
	return errors.Errorf("unimplemented API service call %s.%s", srv, call)
}

func (s *accountingSvcExec) Balance(context.Context, *accounting.BalanceRequestBody) (*accounting.BalanceResponseBody, error) {
	return new(accounting.BalanceResponseBody), nil
}

func (s *sessionSvc) Create(context.Context, *session.CreateRequest) (*session.CreateResponse, error) {
	return nil, unimplementedErr("Session", "Create")
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

func serveGRPC(c *cfg) {
	lis, err := net.Listen("tcp", c.grpcAddr)
	fatalOnErr(err)

	c.grpcSrv = grpc.NewServer()

	initAccountingService(c)

	container.RegisterContainerServiceServer(c.grpcSrv, containerTransport.New(new(containerSvc)))
	sessionGRPC.RegisterSessionServiceServer(c.grpcSrv, sessionTransport.New(new(sessionSvc)))
	object.RegisterObjectServiceServer(c.grpcSrv, objectTransport.New(new(objectSvc)))

	go func() {
		c.wg.Add(1)
		defer func() {
			c.wg.Done()
		}()

		if err := c.grpcSrv.Serve(lis); err != nil {
			fmt.Println("gRPC server error", err)
		}
	}()

	go func() {
		c.wg.Add(1)
		defer func() {
			fmt.Println("gRPC server stopped gracefully")
			fmt.Println("net listener stopped", lis.Addr())
			c.wg.Done()
		}()

		<-c.ctx.Done()

		c.grpcSrv.GracefulStop()
	}()
}
