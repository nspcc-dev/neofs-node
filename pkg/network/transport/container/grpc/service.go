package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	containersvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
)

// Server wraps NeoFS API Container service and
// provides gRPC Container service server interface.
type Server struct {
	srv containersvc.Server
}

// New creates, initializes and returns Server instance.
func New(c containersvc.Server) *Server {
	return &Server{
		srv: c,
	}
}

// Put converts gRPC PutRequest message and passes it to internal Container service.
func (s *Server) Put(ctx context.Context, req *containerGRPC.PutRequest) (*containerGRPC.PutResponse, error) {
	putReq := new(container.PutRequest)
	if err := putReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Put(ctx, putReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*containerGRPC.PutResponse), nil
}

// Delete converts gRPC DeleteRequest message and passes it to internal Container service.
func (s *Server) Delete(ctx context.Context, req *containerGRPC.DeleteRequest) (*containerGRPC.DeleteResponse, error) {
	delReq := new(container.DeleteRequest)
	if err := delReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Delete(ctx, delReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*containerGRPC.DeleteResponse), nil
}

// Get converts gRPC GetRequest message and passes it to internal Container service.
func (s *Server) Get(ctx context.Context, req *containerGRPC.GetRequest) (*containerGRPC.GetResponse, error) {
	getReq := new(container.GetRequest)
	if err := getReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Get(ctx, getReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*containerGRPC.GetResponse), nil
}

// List converts gRPC ListRequest message and passes it to internal Container service.
func (s *Server) List(ctx context.Context, req *containerGRPC.ListRequest) (*containerGRPC.ListResponse, error) {
	listReq := new(container.ListRequest)
	if err := listReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.List(ctx, listReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*containerGRPC.ListResponse), nil
}

// SetExtendedACL converts gRPC SetExtendedACLRequest message and passes it to internal Container service.
func (s *Server) SetExtendedACL(ctx context.Context, req *containerGRPC.SetExtendedACLRequest) (*containerGRPC.SetExtendedACLResponse, error) {
	setEACLReq := new(container.SetExtendedACLRequest)
	if err := setEACLReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.SetExtendedACL(ctx, setEACLReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*containerGRPC.SetExtendedACLResponse), nil
}

// GetExtendedACL converts gRPC GetExtendedACLRequest message and passes it to internal Container service.
func (s *Server) GetExtendedACL(ctx context.Context, req *containerGRPC.GetExtendedACLRequest) (*containerGRPC.GetExtendedACLResponse, error) {
	getEACLReq := new(container.GetExtendedACLRequest)
	if err := getEACLReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.GetExtendedACL(ctx, getEACLReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*containerGRPC.GetExtendedACLResponse), nil
}

// AnnounceUsedSpace converts gRPC AnnounceUsedSpaceRequest message and passes it to internal Container service.
func (s *Server) AnnounceUsedSpace(ctx context.Context, req *containerGRPC.AnnounceUsedSpaceRequest) (*containerGRPC.AnnounceUsedSpaceResponse, error) {
	announceReq := new(container.AnnounceUsedSpaceRequest)
	if err := announceReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.AnnounceUsedSpace(ctx, announceReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*containerGRPC.AnnounceUsedSpaceResponse), nil
}
