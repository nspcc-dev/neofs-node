package object_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"strconv"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	. "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
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
	"google.golang.org/protobuf/encoding/protowire"
)

func TestServer_Get_Local(t *testing.T) {
	signer := usertest.User()
	cnr := cidtest.ID()
	var fsChain nopFSChain
	var mtrc nopMetrics
	var aclChecker nopACLChecker
	var reqInfoExt nopReqInfoExtractor

	storage := newSimpleStorage(t, fsChain)

	handlerFSChain := mockHandlerFSChain{
		repRules:  []uint{3},               // any non-empty
		nodeLists: [][]netmap.NodeInfo{{}}, // any non-empty
	}

	handler := getsvc.New(&handlerFSChain,
		getsvc.WithLocalStorageEngine(storage),
	)
	handlers := &getOnlyHandler{svc: handler}

	srv := New(handlers, 0, nil, fsChain, nil, nil, signer.ECDSAPrivateKey, mtrc, aclChecker, reqInfoExt, nil)

	for _, pldLen := range []uint64{
		0, 1,
		4 << 10, 100 << 10, 256 << 10,
		4 << 20, 10 << 20,
	} {
		t.Run("payload_len="+strconv.FormatUint(pldLen, 10), func(t *testing.T) {
			obj := object.New(cnr, signer.ID)
			obj.SetPayloadSize(pldLen)
			obj.SetPayload(testutil.RandByteSlice(pldLen))
			require.NoError(t, obj.SetVerificationFields(signer))

			require.NoError(t, storage.Put(obj, nil))

			assertGetOK(t, srv, *obj, signer)

			t.Run("cut payload in header", func(t *testing.T) {
				handlers.mockObject = obj.Marshal()

				handlers.mockHeaderLen = len(obj.CutPayload().Marshal())

				t.Run("no payload", func(t *testing.T) {
					assertGetOK(t, srv, *obj, signer)
				})

				if pldLen == 0 {
					return
				}

				t.Run("no payload bytes", func(t *testing.T) {
					tagLen := 1 + protowire.SizeVarint(pldLen)
					for range tagLen {
						handlers.mockHeaderLen++
					}
					assertGetOK(t, srv, *obj, signer)
				})

				handlers.mockHeaderLen = len(handlers.mockObject) - 1
				assertGetOK(t, srv, *obj, signer)
			})

			handlers.mockObject = nil
		})
	}

	t.Run("EC part", func(t *testing.T) {
		const anyRuleIdx = 13
		const anyPartIdx = 42

		handlerFSChain.ecRules = make([]iec.Rule, anyRuleIdx+1)
		handlerFSChain.ecRules[anyRuleIdx].DataPartNum = anyPartIdx/2 + 1
		handlerFSChain.ecRules[anyRuleIdx].ParityPartNum = anyPartIdx/2 + 1

		parentHdr := *object.New(cnr, signer.ID)
		require.NoError(t, parentHdr.SetVerificationFields(signer))

		part, err := iec.FormObjectForECPart(signer, parentHdr, testutil.RandByteSlice(4<<10), iec.PartInfo{
			RuleIndex: anyRuleIdx,
			Index:     anyPartIdx,
		}) // any part payload
		require.NoError(t, err)

		require.NoError(t, storage.Put(&part, nil))

		req := newUnsignedLocalGetRequest(version.Current(), parentHdr.Address())
		req.MetaHeader.XHeaders = []*protosession.XHeader{
			{Key: "__NEOFS__EC_RULE_IDX", Value: strconv.Itoa(anyRuleIdx)},
			{Key: "__NEOFS__EC_PART_IDX", Value: strconv.Itoa(anyPartIdx)},
		}
		signGetRequest(t, req, signer)

		assertGetRequest(t, srv, req, part)

		handlerFSChain.ecRules = nil
	})
}

type getOnlyHandler struct {
	noCallObjectService
	svc *getsvc.Service

	mockObject    []byte
	mockHeaderLen int
}

func (x getOnlyHandler) Get(ctx context.Context, prm getsvc.Prm) error {
	if hdrBuf, submitStreamFn := prm.GetBuffer(); hdrBuf != nil && x.mockObject != nil {
		hdrLen := min(x.mockHeaderLen, len(hdrBuf))
		copy(hdrBuf, x.mockObject[:hdrLen])
		submitStreamFn(hdrLen, io.NopCloser(bytes.NewReader(x.mockObject[hdrLen:])))
		return nil
	}

	return x.svc.Get(ctx, prm)
}

func newLocalGetRequest(t *testing.T, ver version.Version, addr oid.Address, signer neofscrypto.Signer) *protoobject.GetRequest {
	req := newUnsignedLocalGetRequest(ver, addr)
	signGetRequest(t, req, signer)
	return req
}

func newUnsignedLocalGetRequest(ver version.Version, addr oid.Address) *protoobject.GetRequest {
	return &protoobject.GetRequest{
		Body: &protoobject.GetRequest_Body{
			Address: addr.ProtoMessage(),
		},
		MetaHeader: &protosession.RequestMetaHeader{
			Version: ver.ProtoMessage(),
			Ttl:     1,
		},
	}
}

func signGetRequest(t *testing.T, req *protoobject.GetRequest, signer neofscrypto.Signer) {
	var err error
	req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
	require.NoError(t, err)
}

func callGet(t *testing.T, srv *Server, req *protoobject.GetRequest) (grpc.ServerStreamingClient[protoobject.GetResponse], error) {
	// simulating a full gRPC request lifecycle starting from the client
	lis := bufconn.Listen(32 << 10)

	grpcSrv := grpc.NewServer(
		grpc.ForceServerCodecV2(iprotobuf.BufferedCodec{}),
	)
	t.Cleanup(grpcSrv.Stop)

	protoobject.RegisterObjectServiceServer(grpcSrv, srv)

	go func() { _ = grpcSrv.Serve(lis) }()

	c, err := grpc.NewClient("localhost:8080",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // error otherwise
	)
	require.NoError(t, err) // lib misuse, not a request error

	return protoobject.NewObjectServiceClient(c).Get(context.Background(), req)
}

func assertGetOK(t *testing.T, srv *Server, obj object.Object, signer neofscrypto.Signer) {
	t.Run("signed responses", func(t *testing.T) {
		resps := assertGetOKVersioned(t, srv, obj, signer, version.New(2, 17))
		for _, resp := range resps {
			require.NotNil(t, resp.VerifyHeader)
			require.NoError(t, neofscrypto.VerifyResponseWithBuffer(resp, nil))
		}
	})

	resps := assertGetOKVersioned(t, srv, obj, signer, version.Current())
	require.False(t, slices.ContainsFunc(resps, func(resp *protoobject.GetResponse) bool { return resp.VerifyHeader != nil }))
}

func assertGetOKVersioned(t *testing.T, srv *Server, obj object.Object, signer neofscrypto.Signer, ver version.Version) []*protoobject.GetResponse {
	req := newLocalGetRequest(t, ver, obj.Address(), signer)
	return assertGetRequest(t, srv, req, obj)
}

func assertGetRequest(t *testing.T, srv *Server, req *protoobject.GetRequest, obj object.Object) []*protoobject.GetResponse {
	stream, err := callGet(t, srv, req)
	require.NoError(t, err)

	var resps []*protoobject.GetResponse
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}

		resps = append(resps, resp)
	}

	assertGetStreamResponses(t, obj, resps)

	return resps
}

func assertGetStreamResponses(t *testing.T, obj object.Object, resps []*protoobject.GetResponse) {
	require.NotEmpty(t, resps)

	headResp := resps[0]
	require.NotNil(t, headResp)
	require.Nil(t, headResp.MetaHeader)
	require.NotNil(t, headResp.Body)
	require.IsType(t, (*protoobject.GetResponse_Body_Init_)(nil), headResp.Body.ObjectPart)
	in := headResp.Body.ObjectPart.(*protoobject.GetResponse_Body_Init_)
	require.NotNil(t, in)
	require.NotNil(t, in.Init)
	require.NotNil(t, in.Init.ObjectId)
	require.NotNil(t, in.Init.Signature)
	require.NotNil(t, in.Init.Header)

	var restoredHdr object.Object
	require.NoError(t, restoredHdr.FromProtoMessage(&protoobject.Object{
		ObjectId:  in.Init.ObjectId,
		Signature: in.Init.Signature,
		Header:    in.Init.Header,
	}))
	require.Equal(t, *obj.CutPayload(), restoredHdr)

	var chunks [][]byte
	for _, chunkResp := range resps[1:] {
		require.NotNil(t, chunkResp)
		require.Nil(t, chunkResp.MetaHeader)
		require.NotNil(t, chunkResp.Body)
		require.IsType(t, (*protoobject.GetResponse_Body_Chunk)(nil), chunkResp.Body.ObjectPart)
		chunks = append(chunks, chunkResp.Body.ObjectPart.(*protoobject.GetResponse_Body_Chunk).Chunk)
	}

	payloadLen := len(obj.Payload())
	if payloadLen == 0 {
		require.Empty(t, chunks)
		return
	}

	const maxChunkLen = 256 << 10
	for i := range chunks[:len(chunks)-1] {
		if i == 0 {
			require.EqualValues(t, maxChunkLen-1-protowire.SizeVarint(uint64(payloadLen)), len(chunks[i]))
		} else {
			require.EqualValues(t, maxChunkLen, len(chunks[i]))
		}
	}
	require.LessOrEqual(t, len(chunks[len(chunks)-1]), maxChunkLen)

	gotPayload := slices.Concat(chunks...)
	// bytes.Equal checks len equality, but it's more convenient to distinguish this case
	require.EqualValues(t, len(obj.Payload()), len(gotPayload))
	require.True(t, bytes.Equal(obj.Payload(), gotPayload))
}
