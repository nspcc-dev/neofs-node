package object

import (
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	protoencoding "github.com/nspcc-dev/neofs-sdk-go/proto/encoding"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	maxHeadResponseBodyVarintLen  = iobject.MaxHeaderVarintLen
	maxHeaderOffsetInHeadResponse = 1 + maxHeadResponseBodyVarintLen + 1 + iobject.MaxHeaderVarintLen // 1 for iprotobuf.TagBytes1
	headResponseBufferLen         = maxHeaderOffsetInHeadResponse + 2*iobject.NonPayloadFieldsBufferLength

	maxResponseVerificationHeaderLen = 1 << 10

	maxGetResponseChunkLen       = 254 << 10
	maxGetResponseChunkVarintLen = 3
	maxChunkOffsetInGetResponse  = 1 + maxGetResponseChunkVarintLen + // 1 for iprotobuf.TagBytes1
		1 + maxGetResponseChunkVarintLen // 1 for iprotobuf.TagBytes2
	getResponseChunkBufferLen = maxChunkOffsetInGetResponse + maxGetResponseChunkLen + maxResponseVerificationHeaderLen
)

// Fixed message lengths.
const (
	compressedECDSAPublicKeyLen      = smartcontract.PublicKeyLen
	ecdsaWithSHA256SignatureValueLen = 1 + keys.SignatureLen
	ecdsaWithSHA512SignatureLen      = 1 + 1 + compressedECDSAPublicKeyLen +
		1 + 1 + ecdsaWithSHA256SignatureValueLen // scheme is 0
	verificationHeaderECDSAWithSHA512SignatureLen = 1 + 1 + ecdsaWithSHA512SignatureLen
	responseVerificationHeaderECDSAWIthSHA512Len  = verificationHeaderECDSAWithSHA512SignatureLen * 3
)

var defaultGRPCBufferPool = mem.DefaultBufferPool()

var currentVersionResponseMetaHeader []byte

func init() {
	ver := version.Current()
	mjr := ver.Major()
	mnr := ver.Minor()

	verLn := 1 + protowire.SizeVarint(uint64(mjr)) + 1 + protowire.SizeVarint(uint64(mnr))

	b := make([]byte, 1+protowire.SizeBytes(verLn))

	b[0] = iprotobuf.TagBytes1
	off := 1 + binary.PutUvarint(b[1:], uint64(verLn))
	b[off] = iprotobuf.TagVarint1
	off += 1 + binary.PutUvarint(b[off+1:], uint64(mjr))
	b[off] = iprotobuf.TagVarint2
	off += 1 + binary.PutUvarint(b[off+1:], uint64(mnr))

	currentVersionResponseMetaHeader = b[:off]
}

func writeSignatureToBuffer(buf []byte, pubKey []byte, scheme neofscrypto.Scheme, sig []byte) int {
	// key
	buf[0] = iprotobuf.TagBytes1
	off := 1 + binary.PutUvarint(buf[1:], uint64(len(pubKey)))
	off += copy(buf[off:], pubKey)
	// value
	buf[off] = iprotobuf.TagBytes2
	off += 1 + binary.PutUvarint(buf[off+1:], uint64(len(sig)))
	off += copy(buf[off:], sig)
	// scheme
	buf[off] = iprotobuf.TagVarint3
	return off + 1 + binary.PutUvarint(buf[off+1:], uint64(scheme))
}

func (s *Server) signResponse(buf, body, metaHdr []byte) (int, error) {
	signer := neofsecdsa.Signer(s.signer)

	bodySig, err := signer.Sign(body)
	if err != nil {
		return 0, fmt.Errorf("sign body: %w", err)
	}

	metaSig, err := signer.Sign(metaHdr)
	if err != nil {
		return 0, fmt.Errorf("sign meta header: %w", err)
	}

	origSig, err := signer.Sign(nil) // no origin
	if err != nil {
		return 0, fmt.Errorf("sign empty original verification header: %w", err)
	}

	const scheme = neofscrypto.ECDSA_SHA512

	commonPartLen := 1 + protowire.SizeBytes(len(s.pubKeyBytes)) + 1 + protowire.SizeVarint(uint64(scheme)) + 1

	bodyLen := commonPartLen + protowire.SizeBytes(len(bodySig))
	metaLen := commonPartLen + protowire.SizeBytes(len(metaSig))
	origLen := commonPartLen + protowire.SizeBytes(len(origSig))

	// Practically, verification header has constant length, so this could be faster.
	// But since https://github.com/nspcc-dev/neofs-node/issues/3396 this is not worthy of attention.
	fullLen := 1 + protowire.SizeBytes(bodyLen) + 1 + protowire.SizeBytes(metaLen) + 1 + protowire.SizeBytes(origLen)
	if ln := 1 + protowire.SizeBytes(fullLen); ln > maxResponseVerificationHeaderLen {
		return 0, fmt.Errorf("calculated verification header has len %d, expected limit is %d", ln, maxResponseVerificationHeaderLen)
	}

	buf[0] = iprotobuf.TagBytes3
	off := 1 + binary.PutUvarint(buf[1:], uint64(fullLen))
	// body
	buf[off] = iprotobuf.TagBytes1
	off += 1 + binary.PutUvarint(buf[off+1:], uint64(bodyLen))
	off += writeSignatureToBuffer(buf[off:], s.pubKeyBytes, scheme, bodySig)
	// meta
	buf[off] = iprotobuf.TagBytes2
	off += 1 + binary.PutUvarint(buf[off+1:], uint64(metaLen))
	off += writeSignatureToBuffer(buf[off:], s.pubKeyBytes, scheme, metaSig)
	// origin
	buf[off] = iprotobuf.TagBytes3
	off += 1 + binary.PutUvarint(buf[off+1:], uint64(origLen))
	off += writeSignatureToBuffer(buf[off:], s.pubKeyBytes, scheme, origSig)

	return off, nil
}

func (s *Server) writeMetaHeaderToResponseBuffer(buf []byte) (int, int) {
	ln := len(currentVersionResponseMetaHeader)

	buf[0] = iprotobuf.TagBytes2
	off := 1 + binary.PutUvarint(buf[1:], uint64(ln))
	valOff := off
	off += copy(buf[off:], currentVersionResponseMetaHeader)

	return valOff, off
}

func shiftHeaderInHeadResponseBuffer(respBuf, hdrBuf []byte, sigf, hdrf iprotobuf.FieldBounds, direct bool) iprotobuf.FieldBounds {
	sigLen := sigf.To - sigf.From
	hdrLen := hdrf.To - hdrf.From

	hdrWithSigLen := sigLen + hdrLen
	if hdrWithSigLen == 0 {
		return iprotobuf.FieldBounds{}
	}

	// In object: signature#2, header#3. In response body: header#1, signature#2.
	// So, we must change header tag.
	hdrBuf[hdrf.From] = iprotobuf.TagBytes1

	hdrWithSigOff := maxHeaderOffsetInHeadResponse
	if sigLen > 0 {
		if direct {
			if hdrWithSigOff+hdrf.To+sigLen <= len(respBuf) {
				copy(hdrBuf[hdrf.To:], hdrBuf[sigf.From:sigf.To])
				hdrWithSigOff += hdrf.From
			} else { // not expected to ever happen
				tmp := make([]byte, sigLen)
				copy(tmp, hdrBuf[sigf.From:sigf.To])
				copy(hdrBuf, hdrBuf[hdrf.From:hdrf.To])
				copy(hdrBuf[hdrLen:], tmp)
			}
		} else {
			hdrWithSigOff += sigf.From
		}
	} else {
		hdrWithSigOff += hdrf.From
	}

	var bodyf iprotobuf.FieldBounds

	bodyFldPrefixLen := 1 + protowire.SizeVarint(uint64(hdrWithSigLen))

	bodyf.ValueFrom = hdrWithSigOff - bodyFldPrefixLen

	bodyf.From = bodyf.ValueFrom - (1 + protowire.SizeVarint(uint64(bodyFldPrefixLen+hdrWithSigLen)))

	respBuf[bodyf.From] = iprotobuf.TagBytes1 // body
	binary.PutUvarint(respBuf[bodyf.From+1:], uint64(bodyFldPrefixLen+hdrWithSigLen))

	respBuf[bodyf.ValueFrom] = iprotobuf.TagBytes1 // header with signature
	binary.PutUvarint(respBuf[bodyf.ValueFrom+1:], uint64(hdrWithSigLen))

	bodyf.To = hdrWithSigOff + hdrWithSigLen

	return bodyf
}

var headResponseBufferPool = iprotobuf.NewBufferPool(headResponseBufferLen)

func getBufferForHeadResponse() (*iprotobuf.MemBuffer, []byte) {
	item := headResponseBufferPool.Get()
	return item, item.SliceBuffer[maxHeaderOffsetInHeadResponse:]
}

func shiftHeaderInGetResponseBuffer(respBuf, hdrBuf []byte) iprotobuf.FieldBounds {
	bodyValLen := len(hdrBuf)

	bodyFldPrefixLen := 1 + protowire.SizeVarint(uint64(bodyValLen))

	var bodyf iprotobuf.FieldBounds

	bodyf.ValueFrom = maxHeaderOffsetInHeadResponse - bodyFldPrefixLen

	bodyf.From = bodyf.ValueFrom - (1 + protowire.SizeVarint(uint64(bodyFldPrefixLen+bodyValLen)))

	respBuf[bodyf.From] = iprotobuf.TagBytes1 // body
	binary.PutUvarint(respBuf[bodyf.From+1:], uint64(bodyFldPrefixLen+bodyValLen))

	respBuf[bodyf.ValueFrom] = iprotobuf.TagBytes1 // header with signature
	binary.PutUvarint(respBuf[bodyf.ValueFrom+1:], uint64(bodyValLen))

	bodyf.To = maxHeaderOffsetInHeadResponse + bodyValLen

	return bodyf
}

func shiftPayloadChunkInGetResponseBuffer(respBuf []byte, off, ln int) iprotobuf.FieldBounds {
	return shiftPayloadChunkInResponseBuffer(respBuf, iprotobuf.TagBytes2, off, ln)
}

func shiftPayloadChunkInResponseBuffer(respBuf []byte, chunkFldTag byte, off, ln int) iprotobuf.FieldBounds {
	bodyFldPrefixLen := 1 + protowire.SizeVarint(uint64(ln))

	var bodyf iprotobuf.FieldBounds

	bodyf.ValueFrom = off - bodyFldPrefixLen

	bodyf.From = bodyf.ValueFrom - (1 + protowire.SizeVarint(uint64(bodyFldPrefixLen+ln)))

	respBuf[bodyf.From] = iprotobuf.TagBytes1 // body
	binary.PutUvarint(respBuf[bodyf.From+1:], uint64(bodyFldPrefixLen+ln))

	respBuf[bodyf.ValueFrom] = chunkFldTag
	binary.PutUvarint(respBuf[bodyf.ValueFrom+1:], uint64(ln))

	bodyf.To = off + ln

	return bodyf
}

func parseObjectPayloadFieldTag(buf []byte) (int, uint64, error) {
	if len(buf) == 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}

	if buf[0] != iprotobuf.TagBytes4 {
		return 0, 0, fmt.Errorf("invalid tag %d instead of %d", buf[0], iprotobuf.TagBytes4)
	}

	ln, n, err := iprotobuf.ParseVarint(buf[1:])
	if err != nil {
		return 0, 0, err
	}

	return 1 + n, ln, nil
}

var getResponseChunkBufferPool = iprotobuf.NewBufferPool(getResponseChunkBufferLen)

func getBufferForChunkGetResponse() (*iprotobuf.MemBuffer, []byte) {
	item := getResponseChunkBufferPool.Get()
	chunkBuf := item.SliceBuffer[maxChunkOffsetInGetResponse:]
	return item, chunkBuf[:len(chunkBuf)-maxResponseVerificationHeaderLen]
}

func signECDSAWithSHA512(privKey ecdsa.PrivateKey, data []byte) ([]byte, error) {
	sig, err := neofsecdsa.Signer(privKey).Sign(data)
	if err != nil {
		return nil, err
	}

	if len(sig) != ecdsaWithSHA256SignatureValueLen {
		return nil, fmt.Errorf("wrong signature len: expected %d, got %d", ecdsaWithSHA256SignatureValueLen, len(sig))
	}

	return sig, nil
}

func calculateSignatureCountForAPIVersion(apiVersion version.Version) int {
	switch apiVersion.Compare(version.New(2, 25)) {
	default:
		return 1
	case -1:
		return 3
	case 0:
		return 2
	}
}

func calculateRequestVerificationHeaderFieldLen(apiVersion version.Version) int {
	sigCount := calculateSignatureCountForAPIVersion(apiVersion)
	return protoencoding.CalculateRequestVerificationHeaderFieldLength(sigCount * verificationHeaderECDSAWithSHA512SignatureLen)
}

func chooseAPIVersionForNewRequest(remote version.Version) version.Version {
	if cur := version.Current(); cur.Compare(remote) < 0 {
		return cur
	}
	return remote
}

func (s *Server) writeRequestSignatures(reqBuf []byte, bodyWithMetaLen int, body []byte, metaHdr []byte, apiVersion version.Version) error {
	verCmp := apiVersion.Compare(version.New(2, 25))
	if verCmp > 0 {
		reqSig, err := signECDSAWithSHA512(s.signer, reqBuf[:bodyWithMetaLen])
		if err != nil {
			return fmt.Errorf("sign request body + meta header: %w", err)
		}
		protosession.WriteSingleSignatureRequestVerificationHeaderToRequest(reqBuf[bodyWithMetaLen:], s.pubKeyBytes, neofscrypto.ECDSA_SHA512, reqSig)
		return nil
	}

	bodySig, err := signECDSAWithSHA512(s.signer, body)
	if err != nil {
		return fmt.Errorf("sign request body: %w", err)
	}

	metaSig, err := signECDSAWithSHA512(s.signer, metaHdr)
	if err != nil {
		return fmt.Errorf("sign request meta header: %w", err)
	}

	var originSig []byte
	if verCmp < 0 {
		originSig, err = signECDSAWithSHA512(s.signer, nil)
		if err != nil {
			return fmt.Errorf("sign empty request verification header origin: %w", err)
		}
	}

	protosession.WriteMultiSignatureRequestVerificationHeaderToRequest(reqBuf[bodyWithMetaLen:], s.pubKeyBytes, neofscrypto.ECDSA_SHA512, bodySig, metaSig, originSig)

	return nil
}

func encodeRequestProtobuf(body protoencoding.Message, metaHdr protoencoding.Message, verifHdr protoencoding.Message) *[]byte {
	bodyLen := body.MarshaledSize()
	metaHdrLen := metaHdr.MarshaledSize()
	verifHdrLen := verifHdr.MarshaledSize()

	reqLen := protoencoding.CalculateRequestLength(bodyLen, metaHdrLen, verifHdrLen)

	bufItem := defaultGRPCBufferPool.Get(reqLen)

	buf := *bufItem
	off := protoencoding.WriteRequestBodyMessage(buf, body)
	off += protoencoding.WriteRequestMetaHeaderMessage(buf[off:], metaHdr)
	protoencoding.WriteRequestVerificationHeaderMessage(buf[off:], verifHdr)

	return bufItem
}

func (s *Server) makeLocalRequestFromBody(sigCount int, remoteServerAPIVersion version.Version, body protoencoding.Message) (*[]byte, error) {
	bodyLen := body.MarshaledSize()
	writeBodyFn := protoencoding.WriteStablyMarshalledMessageFunc(body)
	return s.makeLocalRequest(sigCount, remoteServerAPIVersion, bodyLen, writeBodyFn, nil)
}

func (s *Server) makeLocalRequest(sigCount int, remoteServerAPIVersion version.Version, bodyLen int, writeBodyFn protoencoding.WriteMessageFunc, xHeaders []string) (*[]byte, error) {
	metaHdrLen := calculateRequestMetaHeaderLen(remoteServerAPIVersion, 1, xHeaders)

	reqLen := protoencoding.CalculateRequestBodyWithMetaHeaderLength(bodyLen, metaHdrLen)
	if sigCount > 0 {
		reqLen += protoencoding.CalculateRequestVerificationHeaderFieldLength(sigCount * verificationHeaderECDSAWithSHA512SignatureLen)
	}

	bufItem := defaultGRPCBufferPool.Get(reqLen)
	buf := *bufItem

	// body
	off := protoencoding.WriteRequestBodyTagAndLength(buf, bodyLen)
	off += writeBodyFn(buf[off:])
	bodySlice := buf[off-bodyLen : off]

	// meta header
	off += writeRequestMetaHeaderToRequest(buf[off:], remoteServerAPIVersion, 1, xHeaders)

	if sigCount == 0 {
		return bufItem, nil
	}

	// verification header
	err := s.writeRequestSignatures(buf, off, bodySlice, buf[off-metaHdrLen:off], remoteServerAPIVersion)
	if err != nil {
		return nil, err
	}

	return bufItem, nil
}
