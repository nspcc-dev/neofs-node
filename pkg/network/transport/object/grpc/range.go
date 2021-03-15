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
		resp.ToGRPCMessage().(*objectGRPC.GetRangeResponse),
	)
}

// GetRange converts gRPC GetRangeRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *Server) GetRange(req *objectGRPC.GetRangeRequest, gStream objectGRPC.ObjectService_GetRangeServer) error {
	getRngReq := new(object.GetRangeRequest)
	if err := getRngReq.FromGRPCMessage(req); err != nil {
		return err
	}

	// TODO: think about how we transport errors through gRPC
	return s.srv.GetRange(
		getRngReq,
		&getRangeStreamerV2{
			ObjectService_GetRangeServer: gStream,
		},
	)
}
