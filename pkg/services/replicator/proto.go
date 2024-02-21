package replicator

import (
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	_ = iota
	fieldNumSigPubKey
	fieldNumSigVal
	fieldNumSigScheme
)

const (
	_ = iota
	fieldNumReplicateObject
	fieldNumReplicateSignature
)

type blankUnaryReplicateRequest struct {
	signer neofscrypto.Signer
	sigVal []byte
}

func newBlankUnaryReplicateRequest(id oid.ID, signer neofscrypto.Signer) (blankUnaryReplicateRequest, error) {
	sig, err := signer.Sign(id[:])
	if err != nil {
		return blankUnaryReplicateRequest{}, err
	}
	return blankUnaryReplicateRequest{
		signer: signer,
		sigVal: sig,
	}, nil
}

type unaryReplicateRequestLayout struct {
	sigFldLen int
	pubKey    []byte
	sigVal    []byte
	sigScheme neofscrypto.Scheme

	fullLen int
}

func unaryReplicateRequestLayoutForObject(r blankUnaryReplicateRequest, objLen int) unaryReplicateRequestLayout {
	var l unaryReplicateRequestLayout
	l.pubKey = neofscrypto.PublicKeyBytes(r.signer.Public())
	l.sigVal = r.sigVal
	l.sigScheme = r.signer.Scheme()

	l.sigFldLen = protowire.SizeTag(fieldNumSigPubKey) + protowire.SizeBytes(len(l.pubKey)) +
		protowire.SizeTag(fieldNumSigVal) + protowire.SizeBytes(len(l.sigVal)) +
		protowire.SizeTag(fieldNumSigScheme) + protowire.SizeVarint(uint64(l.sigScheme))

	l.fullLen = protowire.SizeTag(fieldNumReplicateSignature) + protowire.SizeBytes(l.sigFldLen) +
		protowire.SizeTag(fieldNumReplicateObject) + protowire.SizeBytes(objLen)

	return l
}

func encodeUnaryReplicateRequestWithObject(l unaryReplicateRequestLayout, b []byte) []byte {
	objLen := len(b)

	if cap(b) < l.fullLen {
		b2 := make([]byte, l.fullLen)
		copy(b2[l.fullLen-objLen:], b)
		b = b2
	} else {
		b = b[:l.fullLen]
		copy(b[l.fullLen-objLen:], b)
	}

	// signature
	bp := b[:0]
	bp = protowire.AppendTag(bp, fieldNumReplicateSignature, protowire.BytesType)
	bp = protowire.AppendVarint(bp, uint64(l.sigFldLen))
	bp = protowire.AppendTag(bp, fieldNumSigPubKey, protowire.BytesType)
	bp = protowire.AppendBytes(bp, l.pubKey)
	bp = protowire.AppendTag(bp, fieldNumSigVal, protowire.BytesType)
	bp = protowire.AppendBytes(bp, l.sigVal)
	bp = protowire.AppendTag(bp, fieldNumSigScheme, protowire.VarintType)
	bp = protowire.AppendVarint(bp, uint64(l.sigScheme))
	// object
	bp = protowire.AppendTag(bp, fieldNumReplicateObject, protowire.BytesType)
	bp = protowire.AppendVarint(bp, uint64(objLen))
	// object binary already written above

	return b
}
