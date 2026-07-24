package object_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	. "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestServer_Get_Local(t *testing.T) {
	signer := usertest.User()
	cnr := cidtest.ID()
	var fsChain nopFSChain
	mtrc := new(metricsCollector)
	var aclChecker nopACLChecker
	var reqInfoExt mockReqInfoExtractor

	storage := newSimpleStorage(t, fsChain)

	handlerFSChain := mockHandlerFSChain{
		repRules:  []uint{3},               // any non-empty
		nodeLists: [][]netmap.NodeInfo{{}}, // any non-empty
	}

	handler := getsvc.New(&handlerFSChain,
		getsvc.WithLocalStorageEngine(storage),
	)
	handlers := &getOnlyHandler{svc: handler}

	var policy netmap.PlacementPolicy
	policy.SetReplicas([]netmap.ReplicaDescriptor{{}}) // any non-empty

	reqInfoExt.getRequestInfo.Container.SetPlacementPolicy(policy)

	srv := New(handlers, fsChain, nil, nil, signer.ECDSAPrivateKey, mtrc, aclChecker, reqInfoExt, nil, zap.NewNop())

	var anyAddr oid.Address
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

			require.NoError(t, storage.Put(context.Background(), obj, nil))
			anyAddr = obj.Address()

			assertGetOK(t, srv, mtrc, *obj, signer)

			suffixLen := uint64(17)
			fromSuffix := uint64(0)
			if pldLen > suffixLen {
				fromSuffix = pldLen - suffixLen
			}
			type extendedRangeTest struct {
				name     string
				rng      *protoobject.ExtendedRange
				from, to uint64
			}
			ranges := []extendedRangeTest{
				{name: "suffix", rng: &protoobject.ExtendedRange{LastPos: &suffixLen}, from: fromSuffix, to: pldLen},
			}
			if pldLen > 0 && pldLen <= 4<<10 {
				first := pldLen / 3
				last := min(first+16, pldLen-1)
				lastClipped := pldLen + 17
				ranges = append(ranges,
					extendedRangeTest{name: "bounded", rng: &protoobject.ExtendedRange{FirstPos: &first, LastPos: &last}, from: first, to: last + 1},
					extendedRangeTest{name: "bounded clipped", rng: &protoobject.ExtendedRange{FirstPos: &first, LastPos: &lastClipped}, from: first, to: pldLen},
					extendedRangeTest{name: "from", rng: &protoobject.ExtendedRange{FirstPos: &first}, from: first, to: pldLen},
				)
			}

			for _, tc := range ranges {
				t.Run("extended "+tc.name, func(t *testing.T) {
					req := newUnsignedLocalGetRequest(version.Current(), obj.Address())
					req.Body.ExtendedRange = tc.rng
					signGetRequest(t, req, signer)

					expected := *obj
					expected.SetPayload(obj.Payload()[tc.from:tc.to])
					assertGetStreamResponses(t, expected, collectGetResponses(t, srv, req), false)
					mtrc.reset()
				})
			}

			t.Run("cut payload in header", func(t *testing.T) {
				handlers.mockObject = obj.Marshal()

				handlers.mockHeaderLen = len(obj.CutPayload().Marshal())

				t.Run("no payload", func(t *testing.T) {
					assertGetOK(t, srv, mtrc, *obj, signer)
				})

				if pldLen == 0 {
					return
				}

				t.Run("no payload bytes", func(t *testing.T) {
					tagLen := 1 + protowire.SizeVarint(pldLen)
					for range tagLen {
						handlers.mockHeaderLen++
					}
					assertGetOK(t, srv, mtrc, *obj, signer)
				})

				t.Run("full payload", func(t *testing.T) {
					handlers.mockHeaderLen = len(handlers.mockObject)
					assertGetOK(t, srv, mtrc, *obj, signer)
				})

				handlers.mockHeaderLen = len(handlers.mockObject) - 1
				assertGetOK(t, srv, mtrc, *obj, signer)
			})

			handlers.mockObject = nil
		})
	}

	t.Run("invalid extended range", func(t *testing.T) {
		firstZero := uint64(0)
		firstFive := uint64(5)
		lastZero := uint64(0)
		lastFour := uint64(4)
		for _, tc := range []struct {
			name string
			body *protoobject.GetRequest_Body
		}{
			{name: "both range forms", body: &protoobject.GetRequest_Body{
				Range:         &protoobject.Range{Length: 1},
				ExtendedRange: &protoobject.ExtendedRange{FirstPos: &firstZero},
			}},
			{name: "empty", body: &protoobject.GetRequest_Body{ExtendedRange: new(protoobject.ExtendedRange)}},
			{name: "reversed bounds", body: &protoobject.GetRequest_Body{
				ExtendedRange: &protoobject.ExtendedRange{FirstPos: &firstFive, LastPos: &lastFour},
			}},
			{name: "zero suffix", body: &protoobject.GetRequest_Body{
				ExtendedRange: &protoobject.ExtendedRange{LastPos: &lastZero},
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tc.body.Address = anyAddr.ProtoMessage()
				req := &protoobject.GetRequest{
					Body: tc.body,
					MetaHeader: &protosession.RequestMetaHeader{
						Version: version.Current().ProtoMessage(),
						Ttl:     1,
					},
				}
				signGetRequest(t, req, signer)

				stream, err := callGet(t, srv, req)
				require.NoError(t, err)
				resp, err := stream.Recv()
				require.NoError(t, err)
				require.NotZero(t, resp.GetMetaHeader().GetStatus().GetCode())
			})
		}
	})

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

		require.NoError(t, storage.Put(context.Background(), &part, nil))

		req := newUnsignedLocalGetRequest(version.Current(), parentHdr.Address())
		req.MetaHeader.XHeaders = []*protosession.XHeader{
			{Key: "__NEOFS__EC_RULE_IDX", Value: strconv.Itoa(anyRuleIdx)},
			{Key: "__NEOFS__EC_PART_IDX", Value: strconv.Itoa(anyPartIdx)},
		}
		signGetRequest(t, req, signer)

		assertGetRequest(t, srv, mtrc, req, part, true)

		handlerFSChain.ecRules = nil
	})
}

func TestServer_Get_Remote(t *testing.T) {
	signer := usertest.User()
	cnr := cidtest.ID()
	var fsChain nopFSChain
	var mtrc metricsCollector
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

	require.NoError(t, storage.Put(context.Background(), obj, nil))

	keyStorage := util.NewKeyStorage(&signer.ECDSAPrivateKey, nil, nil)

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
		handlers := getOnlyHandler{svc: handler}

		srv := New(handlers, fsChain, nil, nil, signer.ECDSAPrivateKey, &mtrc, aclChecker, reqInfoExt, nil, zap.NewNop())

		t.Run("object", func(t *testing.T) {
			const payloadLen = 100 << 10
			obj := *object.New(cnr, signer.ID)
			obj.SetAttributes(
				object.NewAttribute("k1", "v1"),
				object.NewAttribute("k2", "v2"),
			)
			obj.SetPayloadSize(payloadLen)
			obj.SetPayload(testutil.RandByteSlice(payloadLen))
			require.NoError(t, obj.SetVerificationFields(signer))

			req := newUnsignedLocalGetRequest(version.Current(), obj.Address())
			req.MetaHeader.Ttl = 2
			signGetRequest(t, req, signer)

			objMsg := obj.ProtoMessage()

			metaHdr := newBlankMetaHeader()
			metaHdr = nestMetaHeader(metaHdr, 5)

			resps := []*protoobject.GetResponse{
				{
					Body: &protoobject.GetResponse_Body{
						ObjectPart: &protoobject.GetResponse_Body_Init_{
							Init: &protoobject.GetResponse_Body_Init{
								ObjectId:  objMsg.ObjectId,
								Signature: objMsg.Signature,
								Header:    objMsg.Header,
							},
						},
					},
					MetaHeader:   metaHdr,
					VerifyHeader: newAnyVerificationHeader(),
				},
			}

			for chunk := range slices.Chunk(obj.Payload(), payloadLen/9) {
				resps = append(resps, &protoobject.GetResponse{
					Body: &protoobject.GetResponse_Body{
						ObjectPart: &protoobject.GetResponse_Body_Chunk{
							Chunk: chunk,
						},
					},
					VerifyHeader: newAnyVerificationHeader(),
				})
			}

			mockConns.setConn(nodes[1], newFixedGetResponsesConn(t, resps))

			gotResps := collectGetResponses(t, srv, req)
			require.Equal(t, len(resps), len(gotResps))
			require.True(t, slices.EqualFunc(resps, gotResps, func(resp *protoobject.GetResponse, gotResp *protoobject.GetResponse) bool {
				return proto.Equal(resp, gotResp)
			}))
		})

		t.Run("extended range", func(t *testing.T) {
			const payloadLen = 100 << 10
			obj := *object.New(cnr, signer.ID)
			obj.SetPayloadSize(payloadLen)
			obj.SetPayload(testutil.RandByteSlice(payloadLen))
			require.NoError(t, obj.SetVerificationFields(signer))

			first := uint64(payloadLen - 3)
			last := uint64(payloadLen + 10)
			req := newUnsignedLocalGetRequest(version.Current(), obj.Address())
			req.Body.ExtendedRange = &protoobject.ExtendedRange{FirstPos: &first, LastPos: &last}
			req.MetaHeader.Ttl = 2
			signGetRequest(t, req, signer)

			objMsg := obj.ProtoMessage()
			resps := []*protoobject.GetResponse{
				{
					Body: &protoobject.GetResponse_Body{
						ObjectPart: &protoobject.GetResponse_Body_Init_{
							Init: &protoobject.GetResponse_Body_Init{
								ObjectId:  objMsg.ObjectId,
								Signature: objMsg.Signature,
								Header:    objMsg.Header,
							},
						},
					},
					MetaHeader:   nestMetaHeader(newBlankMetaHeader(), 5),
					VerifyHeader: newAnyVerificationHeader(),
				},
				{
					Body: &protoobject.GetResponse_Body{
						ObjectPart: &protoobject.GetResponse_Body_Chunk{
							Chunk: obj.Payload()[first:],
						},
					},
					VerifyHeader: newAnyVerificationHeader(),
				},
			}

			mockConns.setConn(nodes[1], newCheckedGetResponsesConn(t, func(got *protoobject.GetRequest) error {
				if got.GetBody().GetRange() != nil {
					return errors.New("legacy range is set")
				}
				if !proto.Equal(req.Body.ExtendedRange, got.GetBody().GetExtendedRange()) {
					return fmt.Errorf("unexpected extended range: %v", got.GetBody().GetExtendedRange())
				}
				return nil
			}, resps))

			gotResps := collectGetResponses(t, srv, req)
			require.Equal(t, len(resps), len(gotResps))
			require.True(t, slices.EqualFunc(resps, gotResps, func(resp *protoobject.GetResponse, gotResp *protoobject.GetResponse) bool {
				return proto.Equal(resp, gotResp)
			}))
		})

		t.Run("split info", func(t *testing.T) {
			req := newUnsignedLocalGetRequest(version.Current(), obj.Address())
			req.Body.Raw = true
			req.MetaHeader.Ttl = 2
			signGetRequest(t, req, signer)

			metaHdr := newBlankMetaHeader()
			metaHdr = nestMetaHeader(metaHdr, 5)

			resps := []*protoobject.GetResponse{
				{
					Body: &protoobject.GetResponse_Body{
						ObjectPart: &protoobject.GetResponse_Body_SplitInfo{
							SplitInfo: newTestSplitInfo(),
						},
					},
					MetaHeader:   metaHdr,
					VerifyHeader: newAnyVerificationHeader(),
				},
			}

			mockConns.setConn(nodes[1], newFixedGetResponsesConn(t, resps))

			gotResps := collectGetResponses(t, srv, req)
			require.Equal(t, len(resps), len(gotResps))
			require.True(t, slices.EqualFunc(resps, gotResps, func(resp *protoobject.GetResponse, gotResp *protoobject.GetResponse) bool {
				return proto.Equal(resp, gotResp)
			}))
		})
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

func assertGetOK(t *testing.T, srv *Server, mtrc *metricsCollector, obj object.Object, signer neofscrypto.Signer) {
	t.Run("signed responses", func(t *testing.T) {
		resps := assertGetOKVersioned(t, srv, mtrc, obj, signer, version.New(2, 17))
		for _, resp := range resps {
			require.NotNil(t, resp.VerifyHeader)
			require.NoError(t, neofscrypto.VerifyResponseWithBuffer(resp, nil))
		}
	})

	resps := assertGetOKVersioned(t, srv, mtrc, obj, signer, version.Current())
	require.False(t, slices.ContainsFunc(resps, func(resp *protoobject.GetResponse) bool { return resp.VerifyHeader != nil }))
}

func assertGetOKVersioned(t *testing.T, srv *Server, mtrc *metricsCollector, obj object.Object, signer neofscrypto.Signer, ver version.Version) []*protoobject.GetResponse {
	req := newLocalGetRequest(t, ver, obj.Address(), signer)
	return assertGetRequest(t, srv, mtrc, req, obj, false)
}

func assertGetRequest(t *testing.T, srv *Server, mtrc *metricsCollector, req *protoobject.GetRequest, obj object.Object, unsingedObject bool) []*protoobject.GetResponse {
	resps := collectGetResponses(t, srv, req)

	assertGetStreamResponses(t, obj, resps, unsingedObject)

	require.EqualValues(t, obj.PayloadSize(), mtrc.getPayloadLen())
	mtrc.reset()

	return resps
}

func collectGetResponses(t *testing.T, srv *Server, req *protoobject.GetRequest) []*protoobject.GetResponse {
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

	return resps
}

func assertGetStreamResponses(t *testing.T, obj object.Object, resps []*protoobject.GetResponse, unsingedObject bool) {
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
	if unsingedObject {
		require.Nil(t, in.Init.Signature)
	} else {
		require.NotNil(t, in.Init.Signature)
	}
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

func newFixedGetResponsesConn(t *testing.T, resps []*protoobject.GetResponse) clientcore.MultiAddressClient {
	return newCheckedGetResponsesConn(t, nil, resps)
}

func newCheckedGetResponsesConn(t *testing.T, check func(*protoobject.GetRequest) error, resps []*protoobject.GetResponse) clientcore.MultiAddressClient {
	return newRawServiceOnlyConn(t, &grpc.ServiceDesc{
		ServiceName: protoobject.ObjectService_ServiceDesc.ServiceName,
		Streams: []grpc.StreamDesc{
			{
				StreamName: "Get",
				Handler: func(srv any, stream grpc.ServerStream) error {
					if check != nil {
						var req protoobject.GetRequest
						if err := stream.RecvMsg(&req); err != nil {
							return fmt.Errorf("receive request: %w", err)
						}
						if err := check(&req); err != nil {
							return fmt.Errorf("check request: %w", err)
						}
					}
					for i := range resps {
						if err := stream.SendMsg(resps[i]); err != nil {
							return fmt.Errorf("send message #%d: %w", i, err)
						}
					}
					return nil
				},
			},
		},
	})
}
