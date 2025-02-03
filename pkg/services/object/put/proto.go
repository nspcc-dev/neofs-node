package putsvc

import (
	"errors"
	"fmt"
	"math"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"google.golang.org/protobuf/encoding/protowire"
)

// TODO: share common code with replication service code

// Signature message fields.
const (
	_ = iota
	fieldNumSigPubKey
	fieldNumSigVal
	fieldNumSigScheme
)

// Object message fields.
const (
	fieldNumObjectPayload = 4
)

// Replication request fields.
const (
	_ = iota
	fieldNumReplicateObject
	fieldNumReplicateSignature
	fieldSignObjectMeta
)

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
	pldFldLen := protowire.SizeTag(fieldNumObjectPayload) + protowire.SizeBytes(pldLen)
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
	res.b = protowire.AppendTag(res.b, fieldNumObjectPayload, protowire.BytesType)
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
	pldFldLen := protowire.SizeTag(fieldNumObjectPayload) + protowire.SizeBytes(pldLen)
	if pldFldLen > math.MaxInt-hdrLen {
		return res, fmt.Errorf("binary object is too big for this server: %d+%d>%d", hdrLen, pldFldLen, math.MaxInt)
	}

	pubKey := neofscrypto.PublicKeyBytes(signer.Public())
	sigScheme := signer.Scheme()

	sigFldLen := protowire.SizeTag(fieldNumSigPubKey) + protowire.SizeBytes(len(pubKey)) +
		protowire.SizeTag(fieldNumSigVal) + protowire.SizeBytes(len(sig)) +
		protowire.SizeTag(fieldNumSigScheme) + protowire.SizeVarint(uint64(sigScheme))
	fullLen := protowire.SizeTag(fieldNumReplicateSignature) + protowire.SizeBytes(sigFldLen) +
		protowire.SizeTag(fieldNumReplicateObject)
	objFldLen := hdrLen + pldFldLen
	if protowire.SizeBytes(objFldLen) > math.MaxInt-fullLen {
		return res, fmt.Errorf("replicate request exceeds server limit %d", math.MaxInt)
	}
	fullLen += protowire.SizeBytes(objFldLen)
	fullLen += protowire.SizeTag(fieldSignObjectMeta) + protowire.SizeVarint(protowire.EncodeBool(signObjectMeta))

	res.b = getPayload()
	if cap(res.b) < fullLen {
		putPayload(res.b)
		res.b = make([]byte, 0, fullLen)
	}

	// meta signature extension flag
	res.b = protowire.AppendTag(res.b, fieldSignObjectMeta, protowire.VarintType)
	res.b = protowire.AppendVarint(res.b, protowire.EncodeBool(signObjectMeta))
	// signature
	res.b = protowire.AppendTag(res.b, fieldNumReplicateSignature, protowire.BytesType)
	res.b = protowire.AppendVarint(res.b, uint64(sigFldLen))
	res.b = protowire.AppendTag(res.b, fieldNumSigPubKey, protowire.BytesType)
	res.b = protowire.AppendBytes(res.b, pubKey)
	res.b = protowire.AppendTag(res.b, fieldNumSigVal, protowire.BytesType)
	res.b = protowire.AppendBytes(res.b, sig)
	res.b = protowire.AppendTag(res.b, fieldNumSigScheme, protowire.VarintType)
	res.b = protowire.AppendVarint(res.b, uint64(sigScheme))
	// object
	res.b = protowire.AppendTag(res.b, fieldNumReplicateObject, protowire.BytesType)
	res.b = protowire.AppendVarint(res.b, uint64(objFldLen))
	res.hdrOff = len(res.b)
	res.b = res.b[:len(res.b)+hdrLen]
	hdrProto.MarshalStable(res.b[res.hdrOff:])
	res.pldFldOff = len(res.b)
	res.b = protowire.AppendTag(res.b, fieldNumObjectPayload, protowire.BytesType)
	res.b = protowire.AppendVarint(res.b, uint64(pldLen))
	res.pldOff = len(res.b)

	return res, nil
}
