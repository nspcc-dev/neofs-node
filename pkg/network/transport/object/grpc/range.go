package object

import (
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
)

type getRangeStreamerV2 struct {
	objectGRPC.ObjectService_GetRangeServer
}

func (s *getRangeStreamerV2) Send(resp *object.GetRangeResponse) error {
	return s.ObjectService_GetRangeServer.Send(
		object.GetRangeResponseToGRPCMessage(resp),
	)
}

// GetRange converts gRPC GetRangeRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *Server) GetRange(req *objectGRPC.GetRangeRequest, gStream objectGRPC.ObjectService_GetRangeServer) error {
	// TODO: think about how we transport errors through gRPC
	return s.srv.GetRange(
		object.GetRangeRequestFromGRPCMessage(req),
		&getRangeStreamerV2{
			ObjectService_GetRangeServer: gStream,
		},
	)
}
