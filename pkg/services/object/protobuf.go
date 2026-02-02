package object

import (
	"encoding/binary"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	maxHeadResponseBodyVarintLen  = iobject.MaxHeaderVarintLen
	maxHeaderOffsetInHeadResponse = 1 + maxHeadResponseBodyVarintLen + 1 + iobject.MaxHeaderVarintLen // 1 for iprotobuf.TagBytes1
	// TODO: test it is sufficient for everything
	headResponseBufferLen = maxHeaderOffsetInHeadResponse + object.MaxHeaderLen*2
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

func writeMetaHeaderToResponseBuffer(b []byte, epoch uint64, st *protostatus.Status) int {
	ln := len(currentVersionResponseMetaHeader) + 1 + protowire.SizeVarint(epoch)
	stLn := st.MarshaledSize()
	if stLn != 0 {
		ln += 1 + protowire.SizeBytes(stLn)
	}

	b[0] = iprotobuf.TagBytes2
	off := 1 + binary.PutUvarint(b[1:], uint64(ln))
	off += copy(b[off:], currentVersionResponseMetaHeader)
	b[off] = iprotobuf.TagVarint2
	off += 1 + binary.PutUvarint(b[off+1:], epoch)
	if st != nil {
		b[off] = iprotobuf.TagBytes6
		off += 1 + binary.PutUvarint(b[off+1:], uint64(stLn))
		st.MarshalStable(b[off:])
		off += stLn
	}

	return off
}

func getBufferForHeadResponse() []byte {
	return make([]byte, headResponseBufferLen)
}

func shiftHeadResponseBuffer(respBuf, hdrBuf []byte, sigf, hdrf iprotobuf.FieldBounds) int {
	if !hdrf.IsMissing() {
		hdrBuf[hdrf.From] = iprotobuf.TagBytes1
	}

	hdrLen := (sigf.To - sigf.From) + (hdrf.To - hdrf.From)

	respBuf[0] = iprotobuf.TagBytes1
	off := 1 + binary.PutUvarint(respBuf[1:], uint64(1+protowire.SizeBytes(hdrLen)))
	respBuf[off] = iprotobuf.TagBytes1
	off += 1 + binary.PutUvarint(respBuf[off+1:], uint64(hdrLen))

	if !sigf.IsMissing() {
		off += copy(respBuf[off:], hdrBuf[sigf.From:sigf.To])
	}
	if !hdrf.IsMissing() {
		off += copy(respBuf[off:], hdrBuf[hdrf.From:hdrf.To])
	}

	return off
}
