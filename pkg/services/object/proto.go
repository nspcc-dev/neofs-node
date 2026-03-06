package object

import (
	"encoding/binary"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	maxHeadResponseBodyVarintLen  = iobject.MaxHeaderVarintLen
	maxHeaderOffsetInHeadResponse = 1 + maxHeadResponseBodyVarintLen + 1 + iobject.MaxHeaderVarintLen // 1 for iprotobuf.TagBytes1
	headResponseBufferLen         = maxHeaderOffsetInHeadResponse + 2*object.MaxHeaderLen
)

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

func writeMetaHeaderToResponseBuffer(b []byte, epoch uint64) (int, int) {
	ln := len(currentVersionResponseMetaHeader)
	if epoch > 0 {
		ln += 1 + protowire.SizeVarint(epoch)
	}

	b[0] = iprotobuf.TagBytes2
	off := 1 + binary.PutUvarint(b[1:], uint64(ln))

	valOff := off

	off += copy(b[off:], currentVersionResponseMetaHeader)

	if epoch > 0 {
		b[off] = iprotobuf.TagVarint2
		off += 1 + binary.PutUvarint(b[off+1:], epoch)
	}

	return valOff, off
}

func writeSignatureToBuffer(b []byte, pubKey []byte, scheme neofscrypto.Scheme, sig []byte) int {
	// key
	b[0] = iprotobuf.TagBytes1
	off := 1 + binary.PutUvarint(b[1:], uint64(len(pubKey)))
	off += copy(b[off:], pubKey)
	// value
	b[off] = iprotobuf.TagBytes2
	off += 1 + binary.PutUvarint(b[off+1:], uint64(len(sig)))
	off += copy(b[off:], sig)
	// scheme
	b[off] = iprotobuf.TagVarint3
	return off + 1 + binary.PutUvarint(b[off+1:], uint64(scheme))
}

func writeVerificationHeaderToResponseBuffer(b []byte, pubKey []byte, scheme neofscrypto.Scheme, bodySig, metaSig, origSig []byte) int {
	commonPartLen := 1 + protowire.SizeBytes(len(pubKey)) + 1 + protowire.SizeVarint(uint64(scheme)) + 1

	bodyLen := commonPartLen + protowire.SizeBytes(len(bodySig))
	metaLen := commonPartLen + protowire.SizeBytes(len(metaSig))
	origLen := commonPartLen + protowire.SizeBytes(len(origSig))

	fullLen := 1 + protowire.SizeBytes(bodyLen) + 1 + protowire.SizeBytes(metaLen) + 1 + protowire.SizeBytes(origLen)

	b[0] = iprotobuf.TagBytes3
	off := 1 + binary.PutUvarint(b[1:], uint64(fullLen))
	// body
	b[off] = iprotobuf.TagBytes1
	off += 1 + binary.PutUvarint(b[off+1:], uint64(bodyLen))
	off += writeSignatureToBuffer(b[off:], pubKey, scheme, bodySig)
	// meta
	b[off] = iprotobuf.TagBytes2
	off += 1 + binary.PutUvarint(b[off+1:], uint64(metaLen))
	off += writeSignatureToBuffer(b[off:], pubKey, scheme, metaSig)
	// origin
	b[off] = iprotobuf.TagBytes3
	off += 1 + binary.PutUvarint(b[off+1:], uint64(origLen))
	off += writeSignatureToBuffer(b[off:], pubKey, scheme, origSig)

	return off
}

func shiftHeaderInHeadResponseBuffer(respBuf, hdrBuf []byte, sigf, hdrf iprotobuf.FieldBounds, direct bool) (int, int) {
	sigLen := sigf.To - sigf.From

	hdrLen := sigLen + (hdrf.To - hdrf.From)

	respBuf[0] = iprotobuf.TagBytes1 // body
	off := 1 + binary.PutUvarint(respBuf[1:], uint64(1+protowire.SizeBytes(hdrLen)))

	valOff := off

	respBuf[off] = iprotobuf.TagBytes1 // header
	off += 1 + binary.PutUvarint(respBuf[off+1:], uint64(hdrLen))

	// In object: signature#2, header#3. In response body: header#1, signature#2.
	// So, we must change header tag.
	hdrBuf[hdrf.From] = iprotobuf.TagBytes1

	if direct && !hdrf.IsMissing() && !sigf.IsMissing() {
		// Here we also need direct field order for signing.
		var tmpSig []byte
		if sigLen > 0 {
			if hdrf.To+sigLen <= len(respBuf) {
				tmpSig = respBuf[len(respBuf)-sigLen:]
			} else {
				tmpSig = make([]byte, sigf.To-sigf.From)
			}
			copy(tmpSig, hdrBuf[sigf.From:sigf.To])
		}

		off += copy(respBuf[off:], hdrBuf[hdrf.From:hdrf.To])

		if sigLen > 0 {
			off += copy(respBuf[off:], tmpSig)
		}

		return valOff, off
	}

	if !sigf.IsMissing() {
		off += copy(respBuf[off:], hdrBuf[sigf.From:sigf.To])
	}
	if !hdrf.IsMissing() {
		off += copy(respBuf[off:], hdrBuf[hdrf.From:hdrf.To])
	}

	return valOff, off
}

var headResponseBufferPool = iprotobuf.NewBufferPool(headResponseBufferLen)

func getBufferForHeadResponse() (*iprotobuf.MemBuffer, []byte) {
	item := headResponseBufferPool.Get()
	return item, item.SliceBuffer[maxHeaderOffsetInHeadResponse:]
}
