package object

import (
	"context"
	"errors"
	"io"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	status "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

const gRPCMaxDataFrameSize = 16 << 10

const maxSignatureLength = 80 // WalletConnect one

const (
	codeInternal          = uint32(1024*status.Section_SECTION_FAILURE_COMMON) + uint32(status.CommonFail_INTERNAL)
	codeAccessDenied      = uint32(1024*status.Section_SECTION_OBJECT) + uint32(status.Object_ACCESS_DENIED)
	codeContainerNotFound = uint32(1024*status.Section_SECTION_CONTAINER) + uint32(status.Container_CONTAINER_NOT_FOUND)
)

var (
	errUnsupportedSignatureScheme = errors.New("unsupported signature scheme")
)

// Node represents NeoFS storage node that is served by [Server].
type Node interface {
	// CompliesContainerStoragePolicy checks whether Node complies with the
	// referenced container's storage policy.
	//
	// Returns [apistatus.ErrContainerNotFound] if referenced container was not
	// found.
	CompliesContainerStoragePolicy(cid.ID) (bool, error)

	// ClientCompliesContainerStoragePolicy checks whether client authenticated by
	// the specified binary-encoded public key complies with the referenced
	// container's storage policy.
	//
	// Returns [apistatus.ErrContainerNotFound] if referenced container was not
	// found.
	ClientCompliesContainerStoragePolicy(bClientPubKey []byte, _ cid.ID) (bool, error)

	// StoreObject saves given NeoFS object from the referenced container into local
	// object storage of the Node. StoreObject is called only when the Node complies
	// with the container's storage policy.
	StoreObject(cnr cid.ID, hdr objectsdk.Object, hdrBin []byte, pldBin []byte, pldDataOff int) error
}

// Server wraps NeoFS API Object service and
// provides gRPC Object service server interface.
type Server struct {
	objectGRPC.ObjectServiceServer // to satisfy grpc.Server.RegisterService

	srv objectSvc.ServiceServer

	log *zap.Logger

	node Node
}

// New creates, initializes and returns Server instance.
func New(c objectSvc.ServiceServer, node Node, lg *zap.Logger) *Server {
	return &Server{
		srv:  c,
		log:  lg,
		node: node,
	}
}

// Put opens internal Object service Put stream and overtakes data from gRPC stream to it.
func (s *Server) Put(gStream objectGRPC.ObjectService_PutServer) error {
	stream, err := s.srv.Put(gStream.Context())
	if err != nil {
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
		return nil, err
	}

	return resp.ToGRPCMessage().(*objectGRPC.GetRangeHashResponse), nil
}
