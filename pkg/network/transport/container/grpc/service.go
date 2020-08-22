package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
)

// Server wraps NeoFS API Container service and
// provides gRPC Container service server interface.
type Server struct {
	srv container.Service
}

// New creates, initializes and returns Server instance.
func New(c container.Service) *Server {
	return &Server{
		srv: c,
	}
}

// Put converts gRPC PutRequest message and passes it to internal Container service.
func (s *Server) Put(ctx context.Context, req *containerGRPC.PutRequest) (*containerGRPC.PutResponse, error) {
	resp, err := s.srv.Put(ctx, container.PutRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return container.PutResponseToGRPCMessage(resp), nil
}

// Delete converts gRPC DeleteRequest message and passes it to internal Container service.
func (s *Server) Delete(ctx context.Context, req *containerGRPC.DeleteRequest) (*containerGRPC.DeleteResponse, error) {
	resp, err := s.srv.Delete(ctx, container.DeleteRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return container.DeleteResponseToGRPCMessage(resp), nil
}

// Get converts gRPC GetRequest message and passes it to internal Container service.
func (s *Server) Get(ctx context.Context, req *containerGRPC.GetRequest) (*containerGRPC.GetResponse, error) {
	resp, err := s.srv.Get(ctx, container.GetRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return container.GetResponseToGRPCMessage(resp), nil
}

// List converts gRPC ListRequest message and passes it to internal Container service.
func (s *Server) List(ctx context.Context, req *containerGRPC.ListRequest) (*containerGRPC.ListResponse, error) {
	resp, err := s.srv.List(ctx, container.ListRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return container.ListResponseToGRPCMessage(resp), nil
}

// SetExtendedACL converts gRPC SetExtendedACLRequest message and passes it to internal Container service.
func (s *Server) SetExtendedACL(ctx context.Context, req *containerGRPC.SetExtendedACLRequest) (*containerGRPC.SetExtendedACLResponse, error) {
	resp, err := s.srv.SetExtendedACL(ctx, container.SetExtendedACLRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return container.SetExtendedACLResponseToGRPCMessage(resp), nil
}

// GetExtendedACL converts gRPC GetExtendedACLRequest message and passes it to internal Container service.
func (s *Server) GetExtendedACL(ctx context.Context, req *containerGRPC.GetExtendedACLRequest) (*containerGRPC.GetExtendedACLResponse, error) {
	resp, err := s.srv.GetExtendedACL(ctx, container.GetExtendedACLRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return container.GetExtendedACLResponseToGRPCMessage(resp), nil
}
