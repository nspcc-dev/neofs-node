package object_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	. "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestServer_Head_Local(t *testing.T) {
	signer := usertest.User()
	cnr := cidtest.ID()
	var fsChain nopFSChain
	var mtrc nopMetrics
	var aclChecker nopACLChecker
	var reqInfoExt nopReqInfoExtractor

	const payloadLen = 100 << 10
	obj := object.New(cnr, signer.ID)
	obj.SetPayloadSize(payloadLen)
	obj.SetPayload(testutil.RandByteSlice(payloadLen))
	require.NoError(t, obj.SetVerificationFields(signer))

	storage := newSimpleStorage(t, fsChain)

	require.NoError(t, storage.Put(obj, nil))

	handler := getsvc.New(nopHandlerFSChain{},
		getsvc.WithLocalStorageEngine(storage),
	)
	handlers := headOnlyHandler{svc: handler}

	srv := New(handlers, 0, nil, fsChain, nil, nil, signer.ECDSAPrivateKey, mtrc, aclChecker, reqInfoExt, nil)

	assertWithVersion := func(t *testing.T, ver version.Version) *protoobject.HeadResponse {
		req := newLocalHeadRequest(t, ver, obj.Address(), signer)

		resp, err := callHead(t, srv, req)
		require.NoError(t, err)

		require.Equal(t, &protosession.ResponseMetaHeader{
			Version: version.Current().ProtoMessage(),
			Epoch:   fsChain.CurrentEpoch(),
			Status:  nil, // for clarity
		}, resp.MetaHeader)

		require.NotNil(t, resp.Body)
		require.Equal(t, &protoobject.HeadResponse_Body{
			Head: &protoobject.HeadResponse_Body_Header{
				Header: &protoobject.HeaderWithSignature{
					Header:    obj.ProtoMessage().Header,
					Signature: obj.Signature().ProtoMessage(),
				},
			},
		}, resp.Body)

		return resp
	}

	t.Run("signed response", func(t *testing.T) {
		resp := assertWithVersion(t, version.New(2, 17))
		require.NotNil(t, resp.VerifyHeader)
		require.NoError(t, neofscrypto.VerifyResponseWithBuffer(resp, nil))
	})

	resp := assertWithVersion(t, version.Current())
	require.Nil(t, resp.VerifyHeader)
}

type headOnlyHandler struct {
	noCallObjectService
	svc *getsvc.Service
}

func (x headOnlyHandler) Head(ctx context.Context, prm getsvc.HeadPrm) error {
	return x.svc.Head(ctx, prm)
}

func newLocalHeadRequest(t *testing.T, ver version.Version, addr oid.Address, signer neofscrypto.Signer) *protoobject.HeadRequest {
	req := &protoobject.HeadRequest{
		Body: &protoobject.HeadRequest_Body{
			Address: addr.ProtoMessage(),
		},
		MetaHeader: &protosession.RequestMetaHeader{
			Version: ver.ProtoMessage(),
			Ttl:     1,
		},
	}

	var err error
	req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
	require.NoError(t, err)

	return req
}

func callHead(t *testing.T, srv *Server, req *protoobject.HeadRequest) (*protoobject.HeadResponse, error) {
	// simulating a full gRPC request lifecycle starting from the client
	lis := bufconn.Listen(32 << 10)

	grpcSrv := grpc.NewServer(
		grpc.ForceServerCodecV2(iprotobuf.BufferedCodec{}),
	)
	t.Cleanup(grpcSrv.Stop)

	grpcSrv.RegisterService(&grpc.ServiceDesc{
		ServiceName: protoobject.ObjectService_ServiceDesc.ServiceName,
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Head",
				Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					s, ok := srv.(*Server)
					if !ok {
						return nil, fmt.Errorf("unexpected server type %T instead of %T", srv, s)
					}
					req := new(protoobject.HeadRequest)
					if err := dec(req); err != nil {
						return nil, fmt.Errorf("decode request: %w", err)
					}
					return s.HeadBuffered(ctx, req), nil
				},
			},
		},
	}, srv)

	go func() { _ = grpcSrv.Serve(lis) }()

	c, err := grpc.NewClient("localhost:8080",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // error otherwise
	)
	require.NoError(t, err) // lib misuse, not a request error

	return protoobject.NewObjectServiceClient(c).Head(context.Background(), req)
}
