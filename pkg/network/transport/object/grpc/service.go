package object

import (
	"context"
	"io"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
)

// Server wraps NeoFS API Object service and
// provides gRPC Object service server interface.
type Server struct {
	srv object.Service
}

// New creates, initializes and returns Server instance.
func New(c object.Service) *Server {
	return &Server{
		srv: c,
	}
}

// Get converts gRPC GetRequest message, opens internal Object service Get stream and overtakes its data
// to gRPC stream.
func (s *Server) Get(req *objectGRPC.GetRequest, gStream objectGRPC.ObjectService_GetServer) error {
	stream, err := s.srv.Get(gStream.Context(), object.GetRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return err
	}

	for {
		r, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}

		if err := gStream.Send(object.GetResponseToGRPCMessage(r)); err != nil {
			return err
		}
	}
}

// Put opens internal Object service Put stream and overtakes data from gRPC stream to it.
func (s *Server) Put(gStream objectGRPC.ObjectService_PutServer) error {
	stream, err := s.srv.Put(gStream.Context())
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return err
	}

	for {
		req, err := gStream.Recv()
		if err == nil {
			if err := stream.Send(object.PutRequestFromGRPCMessage(req)); err != nil {
				return err
			}
		}

		if err == io.EOF {
			resp, err := stream.CloseAndRecv()
			if err != nil {
				return err
			}

			return gStream.SendAndClose(object.PutResponseToGRPCMessage(resp))
		}

		return err
	}
}

// Delete converts gRPC DeleteRequest message and passes it to internal Object service.
func (s *Server) Delete(ctx context.Context, req *objectGRPC.DeleteRequest) (*objectGRPC.DeleteResponse, error) {
	resp, err := s.srv.Delete(ctx, object.DeleteRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return object.DeleteResponseToGRPCMessage(resp), nil
}

// Head converts gRPC HeadRequest message and passes it to internal Object service.
func (s *Server) Head(ctx context.Context, req *objectGRPC.HeadRequest) (*objectGRPC.HeadResponse, error) {
	resp, err := s.srv.Head(ctx, object.HeadRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return object.HeadResponseToGRPCMessage(resp), nil
}

// Search converts gRPC SearchRequest message, opens internal Object service Search stream and overtakes its data
// to gRPC stream.
func (s *Server) Search(req *objectGRPC.SearchRequest, gStream objectGRPC.ObjectService_SearchServer) error {
	stream, err := s.srv.Search(gStream.Context(), object.SearchRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return err
	}

	for {
		r, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}

		if err := gStream.Send(object.SearchResponseToGRPCMessage(r)); err != nil {
			return err
		}
	}
}

// GetRange converts gRPC GetRangeRequest message, opens internal Object service Search stream and overtakes its data
// to gRPC stream.
func (s *Server) GetRange(req *objectGRPC.GetRangeRequest, gStream objectGRPC.ObjectService_GetRangeServer) error {
	stream, err := s.srv.GetRange(gStream.Context(), object.GetRangeRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return err
	}

	for {
		r, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}

		if err := gStream.Send(object.GetRangeResponseToGRPCMessage(r)); err != nil {
			return err
		}
	}
}

// GetRangeHash converts gRPC GetRangeHashRequest message and passes it to internal Object service.
func (s *Server) GetRangeHash(ctx context.Context, req *objectGRPC.GetRangeHashRequest) (*objectGRPC.GetRangeHashResponse, error) {
	resp, err := s.srv.GetRangeHash(ctx, object.GetRangeHashRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return object.GetRangeHashResponseToGRPCMessage(resp), nil
}
