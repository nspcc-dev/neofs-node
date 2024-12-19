package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	protocontainer "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
)

// Server is an interface of the NeoFS API Container service server.
type Server interface {
	Put(context.Context, *container.PutRequest) (*container.PutResponse, error)
	Get(context.Context, *container.GetRequest) (*container.GetResponse, error)
	Delete(context.Context, *container.DeleteRequest) (*container.DeleteResponse, error)
	List(context.Context, *container.ListRequest) (*container.ListResponse, error)
	SetExtendedACL(context.Context, *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error)
	GetExtendedACL(context.Context, *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error)
	AnnounceUsedSpace(context.Context, *container.AnnounceUsedSpaceRequest) (*container.AnnounceUsedSpaceResponse, error)
}

// Server wraps NeoFS API Container service and
// provides gRPC Container service server interface.
type server struct {
	protocontainer.UnimplementedContainerServiceServer
	srv Server
}

// New returns protocontainer.ContainerServiceServer based on the Server.
func New(c Server) protocontainer.ContainerServiceServer {
	return &server{
		srv: c,
	}
}

// Put converts gRPC PutRequest message and passes it to internal Container service.
func (s *server) Put(ctx context.Context, req *protocontainer.PutRequest) (*protocontainer.PutResponse, error) {
	putReq := new(container.PutRequest)
	if err := putReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Put(ctx, putReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protocontainer.PutResponse), nil
}

// Delete converts gRPC DeleteRequest message and passes it to internal Container service.
func (s *server) Delete(ctx context.Context, req *protocontainer.DeleteRequest) (*protocontainer.DeleteResponse, error) {
	delReq := new(container.DeleteRequest)
	if err := delReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Delete(ctx, delReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protocontainer.DeleteResponse), nil
}

// Get converts gRPC GetRequest message and passes it to internal Container service.
func (s *server) Get(ctx context.Context, req *protocontainer.GetRequest) (*protocontainer.GetResponse, error) {
	getReq := new(container.GetRequest)
	if err := getReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Get(ctx, getReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protocontainer.GetResponse), nil
}

// List converts gRPC ListRequest message and passes it to internal Container service.
func (s *server) List(ctx context.Context, req *protocontainer.ListRequest) (*protocontainer.ListResponse, error) {
	listReq := new(container.ListRequest)
	if err := listReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.List(ctx, listReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protocontainer.ListResponse), nil
}

// SetExtendedACL converts gRPC SetExtendedACLRequest message and passes it to internal Container service.
func (s *server) SetExtendedACL(ctx context.Context, req *protocontainer.SetExtendedACLRequest) (*protocontainer.SetExtendedACLResponse, error) {
	setEACLReq := new(container.SetExtendedACLRequest)
	if err := setEACLReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.SetExtendedACL(ctx, setEACLReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protocontainer.SetExtendedACLResponse), nil
}

// GetExtendedACL converts gRPC GetExtendedACLRequest message and passes it to internal Container service.
func (s *server) GetExtendedACL(ctx context.Context, req *protocontainer.GetExtendedACLRequest) (*protocontainer.GetExtendedACLResponse, error) {
	getEACLReq := new(container.GetExtendedACLRequest)
	if err := getEACLReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.GetExtendedACL(ctx, getEACLReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protocontainer.GetExtendedACLResponse), nil
}

// AnnounceUsedSpace converts gRPC AnnounceUsedSpaceRequest message and passes it to internal Container service.
func (s *server) AnnounceUsedSpace(ctx context.Context, req *protocontainer.AnnounceUsedSpaceRequest) (*protocontainer.AnnounceUsedSpaceResponse, error) {
	announceReq := new(container.AnnounceUsedSpaceRequest)
	if err := announceReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.AnnounceUsedSpace(ctx, announceReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protocontainer.AnnounceUsedSpaceResponse), nil
}
