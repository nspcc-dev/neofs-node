package object_test

import (
	"context"
	"encoding/base64"
	"net"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	objecttest "github.com/nspcc-dev/neofs-api-go/v2/object/test"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	objectgrpc "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	protoencoding "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type nopObjectService struct{}

func (nopObjectService) Get(request *objectV2.GetRequest, stream object.GetObjectStream) error {
	return nil
}

type nopObjectPut struct{}

func (nopObjectPut) Send(*objectV2.PutRequest) error {
	return nil
}

func (nopObjectPut) CloseAndRecv() (*objectV2.PutResponse, error) {
	return new(objectV2.PutResponse), nil
}

func (n nopObjectService) Put(ctx context.Context) (object.PutObjectStream, error) {
	return nopObjectPut{}, nil
}

func (n nopObjectService) Head(ctx context.Context, request *objectV2.HeadRequest) (*objectV2.HeadResponse, error) {
	return new(objectV2.HeadResponse), nil
}

func (n nopObjectService) Search(request *objectV2.SearchRequest, stream object.SearchStream) error {
	return nil
}

func (n nopObjectService) Delete(ctx context.Context, request *objectV2.DeleteRequest) (*objectV2.DeleteResponse, error) {
	return new(objectV2.DeleteResponse), nil
}

func (n nopObjectService) GetRange(request *objectV2.GetRangeRequest, stream object.GetObjectRangeStream) error {
	return nil
}

func (n nopObjectService) GetRangeHash(ctx context.Context, request *objectV2.GetRangeHashRequest) (*objectV2.GetRangeHashResponse, error) {
	return new(objectV2.GetRangeHashResponse), nil
}

type binaryCodec struct{}

func (x binaryCodec) Marshal(v any) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return encoding.GetCodec(protoencoding.Name).Marshal(v)
}

func (x binaryCodec) Unmarshal(data []byte, v any) error {
	if bPtr, ok := v.(*[]byte); ok {
		*bPtr = data
		return nil
	}
	return encoding.GetCodec(protoencoding.Name).Unmarshal(data, v)
}

func (x binaryCodec) Name() string {
	return "some_name" // any non-empty
}

type objectPutService struct {
	desc   grpc.StreamDesc
	method string
	conn   *grpc.ClientConn
}

func (x *objectPutService) newStream() (*objectPutBinStream, error) {
	stream, err := x.conn.NewStream(context.Background(), &x.desc, x.method, grpc.ForceCodec(binaryCodec{}))
	if err != nil {
		return nil, err
	}

	return &objectPutBinStream{base: stream}, nil
}

type objectPutBinStream struct {
	base grpc.ClientStream
}

func (x objectPutBinStream) send(msg []byte) error {
	return x.base.SendMsg(msg)
}

func (x objectPutBinStream) close() ([]byte, error) {
	if err := x.base.CloseSend(); err != nil {
		return nil, err
	}
	var resp []byte
	return resp, x.base.RecvMsg(&resp)
}

type objectServicePutServer struct {
	grpc.ServerStream
}

func (x objectServicePutServer) SendAndClose(m *objectGRPC.PutResponse) error {

	return x.ServerStream.SendMsg(m)
}

func (x objectServicePutServer) Recv() (*objectGRPC.PutRequest, error) {
	var bReq []byte
	if err := x.ServerStream.RecvMsg(&bReq); err != nil {
		return nil, err
	}
	var req objectGRPC.PutRequest
	err := proto.Unmarshal(bReq, &req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func serveObjectPut(tb testing.TB, srv *objectgrpc.Server) (*objectPutService, error) {
	lis := bufconn.Listen(1 << 20)

	gSrv := grpc.NewServer(grpc.ForceServerCodec(binaryCodec{}))
	svcDesc := &grpc.ServiceDesc{
		ServiceName: "any service name",
		HandlerType: (*interface {
			Put(gStream objectGRPC.ObjectService_PutServer) error
		})(nil),
		Streams: []grpc.StreamDesc{{
			StreamName: "any stream name",
			Handler: func(srv any, stream grpc.ServerStream) error {
				return srv.(*objectgrpc.Server).Put(objectServicePutServer{ServerStream: stream})
			},
			ServerStreams: false,
			ClientStreams: true,
		}},
	}
	gSrv.RegisterService(svcDesc, srv)

	tb.Cleanup(gSrv.GracefulStop)

	go func() {
		require.NoError(tb, gSrv.Serve(lis))
	}()

	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	require.NoError(tb, err)

	return &objectPutService{
		desc:   svcDesc.Streams[0],
		method: svcDesc.ServiceName + "/" + svcDesc.Streams[0].StreamName,
		conn:   conn,
	}, nil
}

func TestServerPutSignatures(t *testing.T) {
	k, err := keys.NewPrivateKey()
	require.NoError(t, err)

	srv := objectgrpc.New(object.NewSignService(&k.PrivateKey, nopObjectService{}))

	signReq := func(tb testing.TB, req *objectV2.PutRequest) {
		require.NoError(tb, signature.SignServiceMessage(&k.PrivateKey, req))
	}

	for _, tc := range []struct {
		desc         string
		reqModifier  func(testing.TB, *objectV2.PutRequest)
		expectedCode uint32
	}{
		{
			desc:         "unsigned",
			reqModifier:  func(testing.TB, *objectV2.PutRequest) {},
			expectedCode: 1024,
		},
		{
			desc:         "signed",
			reqModifier:  signReq,
			expectedCode: 0,
		},
		{
			desc: "no body signature",
			reqModifier: func(tb testing.TB, req *objectV2.PutRequest) {
				signReq(tb, req)
				req.GetVerificationHeader().SetBodySignature(nil)
			},
			expectedCode: 1024,
		},
		{
			desc: "broken body signature",
			reqModifier: func(tb testing.TB, req *objectV2.PutRequest) {
				signReq(tb, req)
				req.GetVerificationHeader().GetBodySignature().GetSign()[0]++
			},
			expectedCode: 1024,
		},
		{
			desc: "broken public key",
			reqModifier: func(tb testing.TB, req *objectV2.PutRequest) {
				signReq(tb, req)
				req.GetVerificationHeader().GetBodySignature().GetKey()[0]++
			},
			expectedCode: 1024,
		},
		{
			desc: "invalid scheme",
			reqModifier: func(tb testing.TB, req *objectV2.PutRequest) {
				signReq(tb, req)
				req.GetVerificationHeader().GetBodySignature().SetScheme(100)
			},
			expectedCode: 1024,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			svc, err := serveObjectPut(t, srv)
			require.NoError(t, err)

			var ver refs.Version
			version.Current().WriteToV2(&ver)

			req := objecttest.GeneratePutRequest(false)
			req.GetMetaHeader().SetVersion(&ver)
			req.SetVerificationHeader(nil)

			tc.reqModifier(t, req)

			b, err := proto.Marshal(req.ToGRPCMessage().(*objectGRPC.PutRequest))
			require.NoError(t, err)

			stream, err := svc.newStream()
			require.NoError(t, stream.send(b))

			bResp, err := stream.close()
			require.NoError(t, err)

			var resp objectGRPC.PutResponse
			require.NoError(t, proto.Unmarshal(bResp, &resp))

			var respV2 objectV2.PutResponse
			require.NoError(t, respV2.FromGRPCMessage(&resp))
			require.NoError(t, signature.VerifyServiceMessage(&respV2))

			require.EqualValues(t, tc.expectedCode, resp.GetMetaHeader().GetStatus().GetCode())
		})
	}
}

func BenchmarkServerPutSignatures(b *testing.B) {
	k, err := keys.NewPrivateKey()
	require.NoError(b, err)

	srv := objectgrpc.New(object.NewSignService(&k.PrivateKey, nopObjectService{}))

	// signed 'Hello, world!' chunk message
	const b64Req = "Cg8SDUhlbGxvLCB3b3JsZCESgwQKBAgCEAEQDRhkIgoKA2tleRIDdmFsIgoKA2tleRIDdmFsKjgKLgoBARIFCgMBAgMaBAgDGAIiAQIqGQgDEhUKBQoDAQIDEgUKAwECAxIFCgMBAgMSBgoBARIBAjKeAQqRAQqBAQoECAIQARIFCgMBAgMaOAgBEAEaDggBEAEaA2tleSIDdmFsGg4IARABGgNrZXkiA3ZhbCIICAISAQESAQIiCAgCEgEBEgECGjgIARABGg4IARABGgNrZXkiA3ZhbBoOCAEQARoDa2V5IgN2YWwiCAgCEgEBEgECIggIAhIBARIBAhIFCgMBAgMaBAgDGAISCAoBARIBAhgCOoACCgQIAhABEA0YZCIKCgNrZXkSA3ZhbCIKCgNrZXkSA3ZhbCo4Ci4KAQESBQoDAQIDGgQIAxgCIgECKhkIAxIVCgUKAwECAxIFCgMBAgMSBQoDAQIDEgYKAQESAQIyngEKkQEKgQEKBAgCEAESBQoDAQIDGjgIARABGg4IARABGgNrZXkiA3ZhbBoOCAEQARoDa2V5IgN2YWwiCAgCEgEBEgECIggIAhIBARIBAho4CAEQARoOCAEQARoDa2V5IgN2YWwaDggBEAEaA2tleSIDdmFsIggIAhIBARIBAiIICAISAQESAQISBQoDAQIDGgQIAxgCEggKAQESAQIYAkC5CkC5Chq4AgpmCiECnDxsiGoYgoUjiUKfPkrYKFjN7Aap8VcQEGx6K17Nx0QSQQTstw5zx/atdAyI9s1ibSBbW9qwIVzC1OhUQ6aA30ov4S5H+XL5/dngLqojGC9wgyPpfS+JgPcC0UtI/JTWtEezEmYKIQKcPGyIahiChSOJQp8+StgoWM3sBqnxVxAQbHorXs3HRBJBBCaGkLgGgw7Iw1defcxjr8VuItDhbl3PCMObZNT1lqD5IeKexzWUJYWewcOEu2qX/WStkIca8GOrSTFgvY1E2SIaZgohApw8bIhqGIKFI4lCnz5K2ChYzewGqfFXEBBseitezcdEEkEEBw4endVykOxnMfCJXsv5A/b57Iu2zfE4J5Rt0RcGypDOSEyeqAkGOu+UfIYVD6kElURUeiW7PFgtlVl6JqZxzg=="

	bReq, err := base64.StdEncoding.DecodeString(b64Req)
	require.NoError(b, err)

	svc, err := serveObjectPut(b, srv)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stream, err := svc.newStream()
		require.NoError(b, err)

		err = stream.send(bReq)
		require.NoError(b, err)

		_, err = stream.close()
		require.NoError(b, err)
	}
}
