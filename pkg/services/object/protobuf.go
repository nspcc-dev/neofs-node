package object

import (
	"encoding/binary"
	"fmt"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	maxHeadResponseBodyVarintLen  = iobject.MaxHeaderVarintLen
	maxHeaderOffsetInHeadResponse = 1 + maxHeadResponseBodyVarintLen + 1 + iobject.MaxHeaderVarintLen // 1 for iprotobuf.TagBytes1
	// TODO: test it is sufficient for everything
	headResponseBufferLen = maxHeaderOffsetInHeadResponse + object.MaxHeaderLen*2

	// TODO: share header buffers for HEAD and GET
	maxGetResponseHeaderVarintLen = iobject.MaxHeaderVarintLen
	maxHeaderOffsetInGetResponse  = 1 + maxGetResponseHeaderVarintLen + 1 + maxGetResponseHeaderVarintLen // 1 for iprotobuf.TagBytes1
	getResponseHeaderBufferLen    = maxHeaderOffsetInGetResponse + +object.MaxHeaderLen*2

	maxGetResponseChunkLen       = 256 << 10 // we already have such const, share?
	maxGetResponseChunkVarintLen = 3
	maxChunkOffsetInGetResponse  = 1 + maxGetResponseChunkVarintLen + // 1 for iprotobuf.TagBytes1
		1 + maxGetResponseChunkVarintLen // 1 for iprotobuf.TagBytes2
	getResponseChunkBufferLen = maxChunkOffsetInGetResponse + maxGetResponseChunkLen
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

var headResponseBufferPool = iprotobuf.NewBufferPool(headResponseBufferLen)

func getBufferForHeadResponse() (*iprotobuf.MemBuffer, []byte) {
	item := headResponseBufferPool.Get()
	return item, item.SliceBuffer[maxHeaderOffsetInHeadResponse:]
}

var getResponseHeaderBufferPool = iprotobuf.NewBufferPool(getResponseHeaderBufferLen)

func getBufferForGetResponseHeader() (*iprotobuf.MemBuffer, []byte) {
	item := getResponseHeaderBufferPool.Get()
	return item, item.SliceBuffer[maxHeaderOffsetInGetResponse:]
}

func shiftGetResponseHeaderBuffer(respBuf, hdrBuf []byte) int {
	bodyLen := 1 + protowire.SizeBytes(len(hdrBuf))

	respBuf[0] = iprotobuf.TagBytes1
	off := 1 + binary.PutUvarint(respBuf[1:], uint64(bodyLen))
	respBuf[off] = iprotobuf.TagBytes1
	off += 1 + binary.PutUvarint(respBuf[off+1:], uint64(len(hdrBuf)))

	return off + copy(respBuf[off:], hdrBuf)
}

var getResponseChunkBufferPool = iprotobuf.NewBufferPool(getResponseChunkBufferLen)

func getBufferForGetResponseChunk() (*iprotobuf.MemBuffer, []byte) {
	item := getResponseChunkBufferPool.Get()
	return item, item.SliceBuffer[maxChunkOffsetInGetResponse:]
}

func shiftGetResponseChunkBuffer(respBuf, chunk []byte) int {
	bodyLen := 1 + protowire.SizeBytes(len(chunk))

	respBuf[0] = iprotobuf.TagBytes1
	off := 1 + binary.PutUvarint(respBuf[1:], uint64(bodyLen))
	respBuf[off] = iprotobuf.TagBytes2
	off += 1 + binary.PutUvarint(respBuf[off+1:], uint64(len(chunk)))

	return off + copy(respBuf[off:], chunk)
}

func parseHeaderBinary(b []byte) (iprotobuf.FieldBounds, iprotobuf.FieldBounds, iprotobuf.FieldBounds, error) {
	idf, sigf, hdrf, err := iobject.SeekHeaderFields(b)
	if err != nil {
		return idf, sigf, hdrf, fmt.Errorf("restore layout from received binary: %w", err)
	}

	if !idf.IsMissing() {
		m := new(refs.ObjectID)
		if err = proto.Unmarshal(b[idf.ValueFrom:idf.To], m); err != nil {
			return idf, sigf, hdrf, fmt.Errorf("unmarshal ID from received binary: %w", err)
		}
		if err = new(oid.ID).FromProtoMessage(m); err != nil {
			return idf, sigf, hdrf, fmt.Errorf("invalid ID in received binary: %w", err)
		}
	}

	if !sigf.IsMissing() {
		m := new(refs.Signature)
		if err = proto.Unmarshal(b[sigf.ValueFrom:sigf.To], m); err != nil {
			return idf, sigf, hdrf, fmt.Errorf("unmarshal signature from received binary: %w", err)
		}
		if err = new(neofscrypto.Signature).FromProtoMessage(m); err != nil {
			return idf, sigf, hdrf, fmt.Errorf("invalid signature in received binary: %w", err)
		}
	}

	if !hdrf.IsMissing() {
		m := new(protoobject.Header)
		if err = proto.Unmarshal(b[hdrf.ValueFrom:hdrf.To], m); err != nil {
			return idf, sigf, hdrf, fmt.Errorf("unmarshal header from received binary: %w", err)
		}
		if err = new(object.Object).FromProtoMessage(&protoobject.Object{Header: m}); err != nil {
			return idf, sigf, hdrf, fmt.Errorf("invalid header in received binary: %w", err)
		}
	}

	return idf, sigf, hdrf, nil
}
