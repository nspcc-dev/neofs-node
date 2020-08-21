package main

import (
	"context"
	"net"

	accounting "github.com/nspcc-dev/neofs-api-go/v2/accounting/grpc"
	container "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	object "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	session "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type unimplementedServer struct {
	accServer
	cnrServer
	objServer
}

type accServer struct{}

type sesServer struct{}

type cnrServer struct{}

type objServer struct{}

func unimplementedErr(srv, call string) error {
	return errors.Errorf("unimplemented service call %s.%s", srv, call)
}

func (*accServer) Balance(context.Context, *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	return nil, unimplementedErr("Accounting", "Balance")
}

func (*sesServer) Create(context.Context, *session.CreateRequest) (*session.CreateResponse, error) {
	return nil, unimplementedErr("Session", "Create")
}

func (*cnrServer) Put(context.Context, *container.PutRequest) (*container.PutResponse, error) {
	return nil, unimplementedErr("Contianer", "Put")
}

func (*cnrServer) Delete(context.Context, *container.DeleteRequest) (*container.DeleteResponse, error) {
	return nil, unimplementedErr("Contianer", "Delete")
}

func (*cnrServer) Get(context.Context, *container.GetRequest) (*container.GetResponse, error) {
	return nil, unimplementedErr("Contianer", "Get")
}

func (*cnrServer) List(context.Context, *container.ListRequest) (*container.ListResponse, error) {
	return nil, unimplementedErr("Contianer", "List")
}

func (*cnrServer) SetExtendedACL(context.Context, *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	return nil, unimplementedErr("Contianer", "SetExtendedACL")
}

func (*cnrServer) GetExtendedACL(context.Context, *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	return nil, unimplementedErr("Contianer", "GetExtendedACL")
}

func (*objServer) Get(*object.GetRequest, object.ObjectService_GetServer) error {
	return unimplementedErr("Object", "Get")
}

func (*objServer) Put(object.ObjectService_PutServer) error {
	return unimplementedErr("Object", "Put")
}

func (*objServer) Delete(context.Context, *object.DeleteRequest) (*object.DeleteResponse, error) {
	return nil, unimplementedErr("Object", "Delete")
}

func (*objServer) Head(context.Context, *object.HeadRequest) (*object.HeadResponse, error) {
	return nil, unimplementedErr("Object", "Head")
}

func (*objServer) Search(*object.SearchRequest, object.ObjectService_SearchServer) error {
	return unimplementedErr("Object", "Search")
}

func (*objServer) GetRange(*object.GetRangeRequest, object.ObjectService_GetRangeServer) error {
	return unimplementedErr("Object", "GetRange")
}

func (*objServer) GetRangeHash(context.Context, *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	return nil, unimplementedErr("Object", "GetRangeHash")
}

func serveGRPC(c *cfg) error {
	lis, err := net.Listen("tcp", c.grpcAddr)
	fatalOnErr(err)

	srv := grpc.NewServer()

	s := new(unimplementedServer)

	accounting.RegisterAccountingServiceServer(srv, s)
	container.RegisterContainerServiceServer(srv, s)
	session.RegisterSessionServiceServer(srv, s)
	object.RegisterObjectServiceServer(srv, s)

	if err := srv.Serve(lis); err != nil {
		return err
	}

	lis.Close()

	return nil
}
