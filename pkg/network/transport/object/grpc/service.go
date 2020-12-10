package object

import (
	"context"
	"io"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/pkg/errors"
)

// Server wraps NeoFS API Object service and
// provides gRPC Object service server interface.
type Server struct {
	srv objectSvc.ServiceServer
}

// New creates, initializes and returns Server instance.
func New(c objectSvc.ServiceServer) *Server {
	return &Server{
		srv: c,
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
		if err != nil {
			if errors.Is(errors.Cause(err), io.EOF) {
				resp, err := stream.CloseAndRecv()
				if err != nil {
					return err
				}

				return gStream.SendAndClose(object.PutResponseToGRPCMessage(resp))
			}

			return err
		}

		if err := stream.Send(object.PutRequestFromGRPCMessage(req)); err != nil {
			return err
		}
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

// GetRangeHash converts gRPC GetRangeHashRequest message and passes it to internal Object service.
func (s *Server) GetRangeHash(ctx context.Context, req *objectGRPC.GetRangeHashRequest) (*objectGRPC.GetRangeHashResponse, error) {
	resp, err := s.srv.GetRangeHash(ctx, object.GetRangeHashRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return object.GetRangeHashResponseToGRPCMessage(resp), nil
}
