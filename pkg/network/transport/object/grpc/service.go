package object

import (
	"context"
	"errors"
	"io"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
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
			if errors.Is(err, io.EOF) {
				resp, err := stream.CloseAndRecv()
				if err != nil {
					return err
				}

				return gStream.SendAndClose(resp.ToGRPCMessage().(*objectGRPC.PutResponse))
			}

			return err
		}

		putReq := new(object.PutRequest)
		if err := putReq.FromGRPCMessage(req); err != nil {
			return err
		}

		if err := stream.Send(putReq); err != nil {
			if errors.Is(err, util.ErrAbortStream) {
				resp, err := stream.CloseAndRecv()
				if err != nil {
					return err
				}

				return gStream.SendAndClose(resp.ToGRPCMessage().(*objectGRPC.PutResponse))
			}

			return err
		}
	}
}

// Delete converts gRPC DeleteRequest message and passes it to internal Object service.
func (s *Server) Delete(ctx context.Context, req *objectGRPC.DeleteRequest) (*objectGRPC.DeleteResponse, error) {
	delReq := new(object.DeleteRequest)
	if err := delReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Delete(ctx, delReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*objectGRPC.DeleteResponse), nil
}

// Head converts gRPC HeadRequest message and passes it to internal Object service.
func (s *Server) Head(ctx context.Context, req *objectGRPC.HeadRequest) (*objectGRPC.HeadResponse, error) {
	searchReq := new(object.HeadRequest)
	if err := searchReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Head(ctx, searchReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*objectGRPC.HeadResponse), nil
}

// GetRangeHash converts gRPC GetRangeHashRequest message and passes it to internal Object service.
func (s *Server) GetRangeHash(ctx context.Context, req *objectGRPC.GetRangeHashRequest) (*objectGRPC.GetRangeHashResponse, error) {
	hashRngReq := new(object.GetRangeHashRequest)
	if err := hashRngReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.GetRangeHash(ctx, hashRngReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*objectGRPC.GetRangeHashResponse), nil
}
