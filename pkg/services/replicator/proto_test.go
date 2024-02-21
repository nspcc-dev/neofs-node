package replicator

import (
	"testing"

	objectv2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	objectgrpc "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestUnaryReplicateRequest(t *testing.T) {
	// prepare replicated object
	obj := objecttest.Object(t)
	id := oidtest.ID() // can differ with one from obj in this test
	signer := test.RandomSigner(t)

	objBin, err := obj.Marshal()
	require.NoError(t, err)

	// prepare request
	r, err := newBlankUnaryReplicateRequest(id, signer)
	require.NoError(t, err)

	l := unaryReplicateRequestLayoutForObject(r, len(objBin))

	b := encodeUnaryReplicateRequestWithObject(l, objBin)
	require.Len(t, b, l.fullLen)

	// decode request
	var req objectgrpc.ReplicateRequest
	require.NoError(t, proto.Unmarshal(b, &req))

	// check signature
	require.Equal(t, l.pubKey, req.Signature.Key)
	require.Equal(t, l.sigVal, req.Signature.Sign)
	require.EqualValues(t, l.sigScheme, req.Signature.Scheme)

	var sigv2 refs.Signature
	require.NoError(t, sigv2.FromGRPCMessage(req.Signature))
	var sig neofscrypto.Signature
	require.NoError(t, sig.ReadFromV2(sigv2))
	require.True(t, sig.Verify(id[:]))

	// check object
	var objv2 objectv2.Object
	require.NoError(t, objv2.FromGRPCMessage(req.Object))
	obj2 := *object.NewFromV2(&objv2)
	require.Equal(t, obj, obj2)

	// with pre-allocated buffer
	buf := make([]byte, 0, l.fullLen)
	buf = append(buf, objBin...)

	b = encodeUnaryReplicateRequestWithObject(l, buf)
	require.Equal(t, &buf[0], &b[0])
}
