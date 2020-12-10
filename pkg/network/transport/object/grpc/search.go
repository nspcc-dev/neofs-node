package object

import (
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
)

type searchStreamerV2 struct {
	objectGRPC.ObjectService_SearchServer
}

func (s *searchStreamerV2) Send(resp *object.SearchResponse) error {
	return s.ObjectService_SearchServer.Send(
		object.SearchResponseToGRPCMessage(resp),
	)
}

// Search converts gRPC SearchRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *Server) Search(req *objectGRPC.SearchRequest, gStream objectGRPC.ObjectService_SearchServer) error {
	// TODO: think about how we transport errors through gRPC
	return s.srv.Search(
		object.SearchRequestFromGRPCMessage(req),
		&searchStreamerV2{
			ObjectService_SearchServer: gStream,
		},
	)
}
