package object_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	corenetmap "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	. "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
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
	"google.golang.org/protobuf/proto"
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

	var handlerFSChain mockHandlerFSChain

	handler := getsvc.New(&handlerFSChain,
		getsvc.WithLocalStorageEngine(storage),
	)
	handlers := headOnlyHandler{svc: handler}

	srv := New(handlers, 0, nil, fsChain, nil, nil, signer.ECDSAPrivateKey, mtrc, aclChecker, reqInfoExt, nil)

	assertWithVersion := func(t *testing.T, ver version.Version) *protoobject.HeadResponse {
		req := newLocalHeadRequest(t, ver, obj.Address(), signer)
		return assertHeadRequestOK(t, srv, fsChain, req, *obj)
	}

	t.Run("EC part", func(t *testing.T) {
		const anyRuleIdx = 13
		const anyPartIdx = 42

		handlerFSChain.ecRules = make([]iec.Rule, anyRuleIdx+1)
		handlerFSChain.ecRules[anyRuleIdx].DataPartNum = anyPartIdx/2 + 1
		handlerFSChain.ecRules[anyRuleIdx].ParityPartNum = anyPartIdx/2 + 1

		partHdr, err := iec.FormObjectForECPart(signer, *obj, nil, iec.PartInfo{
			RuleIndex: anyRuleIdx,
			Index:     anyPartIdx,
		}) // payload is not needed
		require.NoError(t, err)

		require.NoError(t, storage.Put(&partHdr, nil))

		req := newUnsignedLocalHeadRequest(version.Current(), obj.Address())
		req.MetaHeader.XHeaders = []*protosession.XHeader{
			{Key: "__NEOFS__EC_RULE_IDX", Value: strconv.Itoa(anyRuleIdx)},
			{Key: "__NEOFS__EC_PART_IDX", Value: strconv.Itoa(anyPartIdx)},
		}
		req.MetaHeader.Ttl = 2 // to show it has no effect w/ EC X-headers
		signHeadRequest(t, req, signer)

		assertHeadRequestOK(t, srv, fsChain, req, partHdr)

		handlerFSChain.ecRules = nil
	})

	t.Run("signed response", func(t *testing.T) {
		resp := assertWithVersion(t, version.New(2, 17))
		require.NotNil(t, resp.VerifyHeader)
		require.NoError(t, neofscrypto.VerifyResponseWithBuffer(resp, nil))
	})

	resp := assertWithVersion(t, version.Current())
	require.Nil(t, resp.VerifyHeader)
}

func TestServer_Head_Remote(t *testing.T) {
	signer := usertest.User()
	cnr := cidtest.ID()
	var fsChain nopFSChain
	var mtrc nopMetrics
	var aclChecker nopACLChecker
	var reqInfoExt nopReqInfoExtractor

	const payloadLen = 100 << 10
	obj := object.New(cnr, signer.ID)
	obj.SetAttributes(
		object.NewAttribute("k1", "v1"),
		object.NewAttribute("k2", "v2"),
	)
	obj.SetPayloadSize(payloadLen)
	obj.SetPayload(testutil.RandByteSlice(payloadLen))
	require.NoError(t, obj.SetVerificationFields(signer))

	storage := newSimpleStorage(t, fsChain)

	require.NoError(t, storage.Put(obj, nil))

	keyStorage := util.NewKeyStorage(&signer.ECDSAPrivateKey, nil, nil)

	var handlerFSChain mockHandlerFSChain
	mockConns := newMockConnections()

	handler := getsvc.New(&handlerFSChain,
		getsvc.WithLocalStorageEngine(storage),
		getsvc.WithClientConstructor(mockConns),
		getsvc.WithKeyStorage(keyStorage),
	)
	handlers := headOnlyHandler{svc: handler}

	srv := New(handlers, 0, nil, fsChain, nil, nil, signer.ECDSAPrivateKey, mtrc, aclChecker, reqInfoExt, nil)

	t.Run("EC part", func(t *testing.T) {
		nodes := make([]netmap.NodeInfo, 3)
		for i := range nodes {
			nodes[i].SetPublicKey([]byte("pub_" + strconv.Itoa(i)))
			nodes[i].SetNetworkEndpoints("localhost:" + strconv.Itoa(9090+i)) // any

			mockConns.setConn(nodes[i], emptyRemoteNode{})
		}

		handlerFSChain.ecRules = make([]iec.Rule, 1) // any non-empty
		handlerFSChain.nodeLists = [][]netmap.NodeInfo{nodes}
		handlerFSChain.localPub = nodes[len(nodes)-1].PublicKey()

		partHdr, err := iec.FormObjectForECPart(signer, *obj, testutil.RandByteSlice(32), iec.PartInfo{
			RuleIndex: 13,
			Index:     42,
		}) // any part payload
		require.NoError(t, err)

		require.NoError(t, storage.Put(&partHdr, nil))

		req := newUnsignedLocalHeadRequest(version.Current(), obj.Address())
		req.MetaHeader.Ttl = 2
		signHeadRequest(t, req, signer)

		assertHeadRequestOK(t, srv, fsChain, req, *obj.CutPayload())
	})

	t.Run("REP forwarded", func(t *testing.T) {
		var handlerFSChain mockHandlerFSChain
		mockConns := newMockConnections()

		nodes := make([]netmap.NodeInfo, 2)
		for i := range nodes {
			nodes[i].SetPublicKey([]byte("pub_" + strconv.Itoa(i)))
			nodes[i].SetNetworkEndpoints("localhost:" + strconv.Itoa(9090+i)) // any
		}

		mockConns.setConn(nodes[0], emptyRemoteNode{})

		handlerFSChain.repRules = []uint{uint(len(nodes))}
		handlerFSChain.nodeLists = [][]netmap.NodeInfo{nodes}

		handler := getsvc.New(&handlerFSChain,
			getsvc.WithLocalStorageEngine(newSimpleStorage(t, fsChain)),
			getsvc.WithClientConstructor(mockConns),
			getsvc.WithKeyStorage(keyStorage),
		)
		handlers := headOnlyHandler{svc: handler}

		srv := New(handlers, 0, nil, fsChain, nil, nil, signer.ECDSAPrivateKey, mtrc, aclChecker, reqInfoExt, nil)

		t.Run("header", func(t *testing.T) {
			const payloadLen = 100 << 10
			obj := object.New(cnr, signer.ID)
			obj.SetAttributes(
				object.NewAttribute("k1", "v1"),
				object.NewAttribute("k2", "v2"),
			)
			obj.SetPayloadSize(payloadLen)
			obj.SetPayload(testutil.RandByteSlice(payloadLen))
			require.NoError(t, obj.SetVerificationFields(signer))

			req := newUnsignedLocalHeadRequest(version.Current(), obj.Address())
			req.MetaHeader.Ttl = 2
			signHeadRequest(t, req, signer)

			objMsg := obj.ProtoMessage()

			metaHdr := newBlankMetaHeader()
			nestMetaHeader(metaHdr, 5)

			resp := &protoobject.HeadResponse{
				Body: &protoobject.HeadResponse_Body{
					Head: &protoobject.HeadResponse_Body_Header{
						Header: &protoobject.HeaderWithSignature{
							Header:    objMsg.Header,
							Signature: objMsg.Signature,
						},
					},
				},
				MetaHeader:   metaHdr,
				VerifyHeader: newAnyVerificationHeader(),
			}

			mockConns.setConn(nodes[1], newFixedHeadResponseConn(t, resp))

			gotResp, err := callHead(t, srv, req)
			require.NoError(t, err)
			require.True(t, proto.Equal(resp, gotResp))
		})

		t.Run("split info", func(t *testing.T) {
			req := newUnsignedLocalHeadRequest(version.Current(), obj.Address())
			req.MetaHeader.Ttl = 2
			signHeadRequest(t, req, signer)

			// TODO: share
			metaHdr := newBlankMetaHeader()
			nestMetaHeader(metaHdr, 5)

			resp := &protoobject.HeadResponse{
				Body: &protoobject.HeadResponse_Body{
					Head: &protoobject.HeadResponse_Body_SplitInfo{
						SplitInfo: newTestSplitInfo(),
					},
				},
				MetaHeader:   metaHdr,
				VerifyHeader: newAnyVerificationHeader(),
			}

			mockConns.setConn(nodes[1], newFixedHeadResponseConn(t, resp))

			gotResp, err := callHead(t, srv, req)
			require.NoError(t, err)
			require.True(t, proto.Equal(resp, gotResp))
		})
	})
}

type headOnlyHandler struct {
	noCallObjectService
	svc *getsvc.Service
}

func (x headOnlyHandler) Head(ctx context.Context, prm getsvc.HeadPrm) error {
	return x.svc.Head(ctx, prm)
}

func newLocalHeadRequest(t *testing.T, ver version.Version, addr oid.Address, signer neofscrypto.Signer) *protoobject.HeadRequest {
	req := newUnsignedLocalHeadRequest(ver, addr)
	signHeadRequest(t, req, signer)
	return req
}

func newUnsignedLocalHeadRequest(ver version.Version, addr oid.Address) *protoobject.HeadRequest {
	return &protoobject.HeadRequest{
		Body: &protoobject.HeadRequest_Body{
			Address: addr.ProtoMessage(),
		},
		MetaHeader: &protosession.RequestMetaHeader{
			Version: ver.ProtoMessage(),
			Ttl:     1,
		},
	}
}

func signHeadRequest(t *testing.T, req *protoobject.HeadRequest, signer neofscrypto.Signer) {
	var err error
	req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
	require.NoError(t, err)
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

func assertHeadRequestOK(t *testing.T, srv *Server, fsChain corenetmap.State, req *protoobject.HeadRequest, expObj object.Object) *protoobject.HeadResponse {
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
				Header:    expObj.ProtoMessage().Header,
				Signature: expObj.Signature().ProtoMessage(),
			},
		},
	}, resp.Body)

	return resp
}

func newFixedHeadResponseConn(t *testing.T, resp *protoobject.HeadResponse) clientcore.MultiAddressClient {
	// TODO: try share code with
	// simulating a full gRPC request lifecycle starting from the client
	lis := bufconn.Listen(32 << 10)

	grpcSrv := grpc.NewServer(
		grpc.ForceServerCodecV2(iprotobuf.BufferedCodec{}),
	)
	t.Cleanup(grpcSrv.Stop)

	grpcSrv.RegisterService(&grpc.ServiceDesc{
		ServiceName: protoobject.ObjectService_ServiceDesc.ServiceName,
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Head",
				Handler: func(_ any, _ context.Context, _ func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					return resp, nil
				},
			},
		},
	}, nil)

	go func() { _ = grpcSrv.Serve(lis) }()

	c, err := grpc.NewClient("localhost:8080",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // error otherwise
	)
	require.NoError(t, err) // lib misuse, not a request error

	return &mockGRPCConn{
		conn: c,
	}
}
