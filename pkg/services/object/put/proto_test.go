package putsvc

import (
	"crypto/rand"
	"testing"

	objectv2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	objectgrpc "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestUnaryReplicateRequest(t *testing.T) {
	// prepare replicated object
	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)
	obj := objecttest.Object()
	obj.SetPayload(payload)
	id := oidtest.ID()
	obj.SetID(id)
	hdr := *obj.CutPayload()
	obj.SetPayloadSize(uint64(len(payload)))
	signer := neofscryptotest.Signer()

	// prepare request
	r, err := encodeReplicateRequestWithoutPayload(signer, hdr, len(payload), true)
	require.NoError(t, err)
	require.Equal(t, len(payload), cap(r.b)-r.pldOff)
	require.Equal(t, len(payload), cap(r.b)-len(r.b))

	r.b = append(r.b, payload...)

	// decode request
	var req objectgrpc.ReplicateRequest
	require.NoError(t, proto.Unmarshal(r.b, &req))

	// check signature
	require.Equal(t, neofscrypto.PublicKeyBytes(signer.Public()), req.Signature.Key)
	require.EqualValues(t, signer.Scheme(), req.Signature.Scheme)

	var sigv2 refs.Signature
	require.NoError(t, sigv2.FromGRPCMessage(req.Signature))
	var sig neofscrypto.Signature
	require.NoError(t, sig.ReadFromV2(sigv2))
	require.True(t, sig.Verify(id[:]))

	// check object
	var objv2 objectv2.Object
	require.NoError(t, objv2.FromGRPCMessage(req.Object))
	var obj2 object.Object
	require.NoError(t, obj2.ReadFromV2(objv2))
	require.Equal(t, obj, obj2)

	// check meta signature flag
	require.True(t, req.GetSignObject())
}
