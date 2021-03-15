package object

import (
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
)

type getStreamerV2 struct {
	objectGRPC.ObjectService_GetServer
}

func (s *getStreamerV2) Send(resp *object.GetResponse) error {
	return s.ObjectService_GetServer.Send(
		resp.ToGRPCMessage().(*objectGRPC.GetResponse),
	)
}

// Get converts gRPC GetRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *Server) Get(req *objectGRPC.GetRequest, gStream objectGRPC.ObjectService_GetServer) error {
	getReq := new(object.GetRequest)
	if err := getReq.FromGRPCMessage(req); err != nil {
		return err
	}

	// TODO: think about how we transport errors through gRPC
	return s.srv.Get(
		getReq,
		&getStreamerV2{
			ObjectService_GetServer: gStream,
		},
	)
}
