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
		resp.ToGRPCMessage().(*objectGRPC.SearchResponse),
	)
}

// Search converts gRPC SearchRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *Server) Search(req *objectGRPC.SearchRequest, gStream objectGRPC.ObjectService_SearchServer) error {
	searchReq := new(object.SearchRequest)
	if err := searchReq.FromGRPCMessage(req); err != nil {
		return err
	}

	// TODO: think about how we transport errors through gRPC
	return s.srv.Search(
		searchReq,
		&searchStreamerV2{
			ObjectService_SearchServer: gStream,
		},
	)
}
