package putsvc

import (
	"errors"
	"fmt"
	"math"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"google.golang.org/protobuf/encoding/protowire"
)

// TODO: share common code with replication service code

type encodedObject struct {
	b []byte

	hdrOff    int
	pldFldOff int
	pldOff    int
}

func encodeObjectWithoutPayload(hdr object.Object, pldLen int) (encodedObject, error) {
	var res encodedObject

	hdrProto := hdr.ProtoMessage()
	hdrLen := hdrProto.MarshaledSize()
	pldFldLen := protowire.SizeTag(protoobject.FieldObjectPayload) + protowire.SizeBytes(pldLen)
	if pldFldLen > math.MaxInt-hdrLen {
		return res, fmt.Errorf("binary object is too big for this server: %d+%d>%d", hdrLen, pldLen, math.MaxInt)
	}

	res.b = getPayload()
	if cap(res.b) < hdrLen+pldFldLen {
		putPayload(res.b)
		res.b = make([]byte, 0, hdrLen+pldFldLen)
	}

	res.b = res.b[:hdrLen]
	hdrProto.MarshalStable(res.b)
	res.pldFldOff = len(res.b)
	res.b = protowire.AppendTag(res.b, protoobject.FieldObjectPayload, protowire.BytesType)
	res.b = protowire.AppendVarint(res.b, uint64(pldLen))
	res.pldOff = len(res.b)

	return res, nil
}

func encodeReplicateRequestWithoutPayload(signer neofscrypto.Signer, hdr object.Object, pldLen int, signObjectMeta bool) (encodedObject, error) {
	var res encodedObject
	id := hdr.GetID()
	if id.IsZero() {
		return res, errors.New("missing object ID")
	}

	sig, err := signer.Sign(id[:])
	if err != nil {
		return res, fmt.Errorf("sign object ID: %w", err)
	}

	hdrProto := hdr.ProtoMessage()
	hdrLen := hdrProto.MarshaledSize()
	pldFldLen := protowire.SizeTag(protoobject.FieldObjectPayload) + protowire.SizeBytes(pldLen)
	if pldFldLen > math.MaxInt-hdrLen {
		return res, fmt.Errorf("binary object is too big for this server: %d+%d>%d", hdrLen, pldFldLen, math.MaxInt)
	}

	pubKey := neofscrypto.PublicKeyBytes(signer.Public())
	sigScheme := signer.Scheme()

	sigFldLen := protowire.SizeTag(protorefs.FieldSignatureKey) + protowire.SizeBytes(len(pubKey)) +
		protowire.SizeTag(protorefs.FieldSignatureValue) + protowire.SizeBytes(len(sig)) +
		protowire.SizeTag(protorefs.FieldSignatureScheme) + protowire.SizeVarint(uint64(sigScheme))
	fullLen := protowire.SizeTag(protoobject.FieldReplicateRequestSignature) + protowire.SizeBytes(sigFldLen) +
		protowire.SizeTag(protoobject.FieldReplicateRequestObject)
	objFldLen := hdrLen + pldFldLen
	if protowire.SizeBytes(objFldLen) > math.MaxInt-fullLen {
		return res, fmt.Errorf("replicate request exceeds server limit %d", math.MaxInt)
	}
	fullLen += protowire.SizeBytes(objFldLen)
	fullLen += protowire.SizeTag(protoobject.FieldReplicateRequestSignObject) + protowire.SizeVarint(protowire.EncodeBool(signObjectMeta))

	res.b = getPayload()
	if cap(res.b) < fullLen {
		putPayload(res.b)
		res.b = make([]byte, 0, fullLen)
	}

	// meta signature extension flag
	res.b = protowire.AppendTag(res.b, protoobject.FieldReplicateRequestSignObject, protowire.VarintType)
	res.b = protowire.AppendVarint(res.b, protowire.EncodeBool(signObjectMeta))
	// signature
	res.b = protowire.AppendTag(res.b, protoobject.FieldReplicateRequestSignature, protowire.BytesType)
	res.b = protowire.AppendVarint(res.b, uint64(sigFldLen))
	res.b = protowire.AppendTag(res.b, protorefs.FieldSignatureKey, protowire.BytesType)
	res.b = protowire.AppendBytes(res.b, pubKey)
	res.b = protowire.AppendTag(res.b, protorefs.FieldSignatureValue, protowire.BytesType)
	res.b = protowire.AppendBytes(res.b, sig)
	res.b = protowire.AppendTag(res.b, protorefs.FieldSignatureScheme, protowire.VarintType)
	res.b = protowire.AppendVarint(res.b, uint64(sigScheme))
	// object
	res.b = protowire.AppendTag(res.b, protoobject.FieldReplicateRequestObject, protowire.BytesType)
	res.b = protowire.AppendVarint(res.b, uint64(objFldLen))
	res.hdrOff = len(res.b)
	res.b = res.b[:len(res.b)+hdrLen]
	hdrProto.MarshalStable(res.b[res.hdrOff:])
	res.pldFldOff = len(res.b)
	res.b = protowire.AppendTag(res.b, protoobject.FieldObjectPayload, protowire.BytesType)
	res.b = protowire.AppendVarint(res.b, uint64(pldLen))
	res.pldOff = len(res.b)

	return res, nil
}
