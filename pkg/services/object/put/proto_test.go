package putsvc

import (
	"crypto/rand"
	"testing"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestUnaryReplicateRequest(t *testing.T) {
	// prepare replicated object
	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)
	obj := objecttest.Object()
	obj.SetPayload(payload)
	obj.SetPayloadSize(uint64(len(payload)))
	id := oidtest.ID()
	obj.SetID(id)
	hdr := *obj.CutPayload()
	signer := neofscryptotest.Signer()

	// prepare request
	r, err := encodeReplicateRequestWithoutPayload(signer, hdr, len(payload), true)
	require.NoError(t, err)
	require.Equal(t, len(payload), cap(r.b)-r.pldOff)
	require.Equal(t, len(payload), cap(r.b)-len(r.b))

	r.b = append(r.b, payload...)

	// decode request
	var req protoobject.ReplicateRequest
	require.NoError(t, proto.Unmarshal(r.b, &req))

	// check signature
	require.Equal(t, neofscrypto.PublicKeyBytes(signer.Public()), req.Signature.Key)
	require.EqualValues(t, signer.Scheme(), req.Signature.Scheme)

	var sig neofscrypto.Signature
	require.NoError(t, sig.FromProtoMessage(req.Signature))
	require.True(t, sig.Verify(id[:]))

	// check object
	var obj2 object.Object
	require.NoError(t, obj2.FromProtoMessage(req.Object))
	require.Equal(t, obj, obj2)

	// check meta signature flag
	require.True(t, req.GetSignObject())
}
