package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

var (
	getStreamDesc = &grpc.StreamDesc{
		StreamName:    "Get",
		ServerStreams: true,
		ClientStreams: false,
	}
	getRangeStreamDesc = &grpc.StreamDesc{
		StreamName:    "GetRange",
		ServerStreams: true,
		ClientStreams: false,
	}
)

type getStreamProgress struct {
	headWas     bool
	readPayload int
}

func callServerStream(ctx context.Context, conn *grpc.ClientConn, method string, streamDesc *grpc.StreamDesc, request any) (grpc.ClientStream, error) {
	stream, err := conn.NewStream(ctx, streamDesc, method,
		grpc.StaticMethod(),
		grpc.ForceCodecV2(iprotobuf.BufferedCodec{}),
	)
	if err != nil {
		return nil, fmt.Errorf("stream opening failed: %w", err)
	}

	if err = stream.SendMsg(request); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	if err = stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("close send: %w", err)
	}

	return stream, nil
}

func callGet(ctx context.Context, conn *grpc.ClientConn, request any) (grpc.ClientStream, error) {
	return callServerStream(ctx, conn, protoobject.ObjectService_Get_FullMethodName, getStreamDesc, request)
}

func callRange(ctx context.Context, conn *grpc.ClientConn, request any) (grpc.ClientStream, error) {
	return callServerStream(ctx, conn, protoobject.ObjectService_GetRange_FullMethodName, getRangeStreamDesc, request)
}

// returns:
//   - nil on completed object transmission
//   - [object.SplitInfoError]/nil on split info response and unset/set raw flag in request
//   - [apistatus.ErrObjectNotFound] on 404 status
//   - nil on other API statuses
//   - any other transport/protocol error otherwise
func (x *getProxyContext) continueWithConn(ctx context.Context, conn *grpc.ClientConn) error {
	stream, err := callGet(ctx, conn, x.req)
	if err != nil {
		return err
	}

	var prog getStreamProgress
	for {
		var respBuf mem.BufferSlice
		if err = stream.RecvMsg(&respBuf); err != nil {
			if errors.Is(err, io.EOF) {
				return x.validateEOF(prog)
			}
			return fmt.Errorf("reading the response failed: %w", err)
		}

		fin, sent, err := x.handleGetResponse(ctx, &prog, respBuf)
		if !sent {
			respBuf.Free()
		}
		if err != nil {
			return fmt.Errorf("handle next stream message: %w", err)
		}
		if fin {
			return nil
		}
	}
}

func (x *getProxyContext) validateEOF(prog getStreamProgress) error {
	reqBody := x.req.GetBody()
	if reqRange := reqBody.GetRange(); reqRange != nil && reqRange.GetLength() > 0 {
		if uint64(x.respondedPayload) < reqRange.GetLength() {
			return io.ErrUnexpectedEOF
		}
	} else if prog.headWas && x.payloadLenCheck > 0 && uint64(x.respondedPayload) < x.payloadLenCheck {
		return io.ErrUnexpectedEOF
	}
	if !reqBody.GetPayloadOnly() && !prog.headWas {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func handleResponseCodeAndBody(respBuf mem.BufferSlice) (uint32, iprotobuf.BuffersSlice, error) {
	var code uint32
	var body iprotobuf.BuffersSlice

	var opts protoscan.ScanMessageOptions
	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		switch num {
		default:
			return protoscan.ErrContinue
		case iprotobuf.FieldResponseBody:
			body = buffers
			return nil
		case iprotobuf.FieldResponseMetaHeader:
			var err error
			code, err = getStatusCodeFromResponseMetaHeader(buffers)
			if err != nil {
				return fmt.Errorf("handle meta header: %w", err)
			}
			return nil
		}
	}

	err := protoscan.ScanMessage(iprotobuf.NewBuffersSlice(respBuf), protoscan.ResponseScheme, opts)

	return code, body, err
}

func (x *getProxyContext) handleGetResponse(ctx context.Context, streamProg *getStreamProgress, respBuf mem.BufferSlice) (bool, bool, error) {
	code, body, err := handleResponseCodeAndBody(respBuf)
	if err != nil {
		return false, false, err
	}

	if code == protostatus.ObjectNotFound {
		return false, false, apistatus.ErrObjectNotFound
	}

	if code != protostatus.OK {
		return true, true, x.respStream.base.SendMsg(respBuf)
	}

	// TODO: forbid body if code != OK?

	sent, err := x.handleResponseBody(ctx, streamProg, respBuf, body)
	if err != nil {
		return false, sent, fmt.Errorf("handle body: %w", err)
	}

	return false, sent, nil
}

func handleGetResponseBodyOneof(headWas *bool, buffers iprotobuf.BuffersSlice) (protowire.Number, iprotobuf.BuffersSlice, error) {
	var oneofNum protowire.Number
	var oneofFld iprotobuf.BuffersSlice

	var opts protoscan.ScanMessageOptions
	opts.InterceptBytes = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		if num == protoobject.FieldGetResponseBodyChunk {
			if !*headWas {
				return errors.New("incorrect message sequence")
			}
			oneofNum, oneofFld = num, buffers
		}
		return nil
	}
	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		switch num {
		default:
			return protoscan.ErrContinue
		case protoobject.FieldGetResponseBodyInit:
			if *headWas {
				return errors.New("incorrect message sequence")
			}
			*headWas = true
			oneofNum, oneofFld = num, buffers
			return nil
		case protoobject.FieldGetResponseBodySplitInfo:
			*headWas = true
			oneofNum, oneofFld = num, buffers
			return nil
		}
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectGetResponseBodyScheme, opts)

	return oneofNum, oneofFld, err
}

func handleRangeResponseBodyOneof(buffers iprotobuf.BuffersSlice) (protowire.Number, iprotobuf.BuffersSlice, error) {
	var oneofNum protowire.Number
	var oneofFld iprotobuf.BuffersSlice

	var opts protoscan.ScanMessageOptions
	opts.InterceptBytes = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		if num == protoobject.FieldRangeResponseBodyChunk {
			oneofNum, oneofFld = num, buffers
		}
		return nil
	}
	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		if num != protoobject.FieldRangeResponseBodySplitInfo {
			return protoscan.ErrContinue
		}
		oneofNum, oneofFld = num, buffers
		return nil
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectGetRangeResponseBodyScheme, opts)

	return oneofNum, oneofFld, err
}

func (x *getProxyContext) handleResponseBody(ctx context.Context, streamProg *getStreamProgress, respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
	oneofNum, oneofFld, err := handleGetResponseBodyOneof(&streamProg.headWas, buffers)
	if err != nil {
		return false, err
	}

	switch oneofNum {
	default:
		return false, errors.New("none of the supported oneof fields are specified")
	case protoobject.FieldGetResponseBodyInit:
		return x.handleInitResponse(ctx, respBuf, oneofFld)
	case protoobject.FieldGetResponseBodyChunk:
		return x.handleChunkResponse(streamProg, respBuf, oneofFld)
	case protoobject.FieldGetResponseBodySplitInfo:
		return x.handleSplitInfo(respBuf, oneofFld)
	}
}

func (x *getProxyContext) handleInitResponse(ctx context.Context, respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
	var hdr iprotobuf.BuffersSlice

	var opts protoscan.ScanMessageOptions
	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		if num != protoobject.FieldGetResponseBodyInitHeader {
			return protoscan.ErrContinue
		}

		var opts protoscan.ScanMessageOrderedOptions
		opts.InterceptUint64 = func(num protowire.Number, u uint64) error {
			if num == protoobject.FieldHeaderPayloadLength {
				x.payloadLenCheck = u
			}
			return nil
		}
		opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice, checkOrder bool) (bool, error) {
			if num != protoobject.FieldHeaderPayloadHash {
				return checkOrder, protoscan.ErrContinue
			}
			var opts protoscan.ScanMessageOrderedOptions
			opts.InterceptBytes = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
				if num == protorefs.FieldChecksumValue {
					x.payloadHashCheck = buffers.ReadOnlyData()
				}
				return nil
			}
			return protoscan.ScanMessageOrdered(buffers, protoscan.ChecksumScheme, opts)
		}

		hdrOrdered, err := protoscan.ScanMessageOrdered(buffers, protoscan.ObjectHeaderScheme, protoscan.ScanMessageOrderedOptions{})
		if err != nil {
			return fmt.Errorf("handle header with signature field: %w", err)
		}

		if err = checkHeaderProtobufAgainstID(buffers, x.reqOID, hdrOrdered); err != nil {
			return err
		}

		hdr = buffers
		return nil
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectGetResponseInitScheme, opts)
	if err != nil {
		return false, err
	}

	var sent bool
	x.onceHdr.Do(func() {
		if x.respStream.recheckEACL {
			err = x.respStream.srv.aclChecker.CheckEACL(ctx, hdr.ReadOnlyData(), x.respStream.reqInfo)
			if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
				err = eACLErr(x.respStream.reqInfo, err)
				return
			}
		}
		if x.suppressInit {
			return
		}
		err = x.respStream.base.SendMsg(respBuf)
		sent = true
	})

	return sent, err
}

func (x *getProxyContext) handleChunkResponse(streamProg *getStreamProgress, respBuf mem.BufferSlice, chunkBuffers iprotobuf.BuffersSlice) (bool, error) {
	chunkLen := chunkBuffers.Len()

	from, to := chunkBoundsToSend(x.respondedPayload, streamProg.readPayload, chunkLen)
	if from == to {
		streamProg.readPayload += chunkLen
		return false, nil
	}

	_, ok := chunkBuffers.MoveNext(from)
	if !ok {
		return false, fmt.Errorf("seek chunk left bound in response buffers: %w", io.ErrUnexpectedEOF)
	}

	chunkBuffers, ok = chunkBuffers.MoveNext(to - from)
	if !ok {
		return false, fmt.Errorf("seek chunk right bound in response buffers: %w", io.ErrUnexpectedEOF)
	}

	if x.payloadHashGot == nil {
		x.payloadHashGot = sha256.New()
	}

	if _, err := chunkBuffers.WriteTo(x.payloadHashGot); err != nil { // should never happen according to hash.Hash docs
		return false, fmt.Errorf("hash payload chunk: %w", err)
	}

	respChunkLen := to - from

	if uint64(x.respondedPayload+respChunkLen) == x.payloadLenCheck {
		if !bytes.Equal(x.payloadHashGot.Sum(nil), x.payloadHashCheck) { // not merged via && for readability
			return false, errors.New("received payload mismatches checksum from header")
		}
	}

	return x.respStream.srv.sendChunkResponse(x.respStream.base, respBuf, chunkBuffers, respChunkLen, chunkLen,
		x.respStream.signResponse, iprotobuf.TagBytes2, &streamProg.readPayload, &x.respondedPayload, shiftPayloadChunkInGetResponseBuffer)
}

func (x *getProxyContext) handleSplitInfo(respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
	return handleSplitInfoAndRespond(x.req.GetBody().GetRaw(), x.respStream.base, respBuf, buffers)
}

type preparedRangeRequest struct {
	offset uint64
	length uint64
	buffer mem.Buffer
}

type getECTransport struct {
	server         *Server
	request        *protoobject.GetRequest
	signResponses  bool
	responseStream grpc.ServerStream

	getPartRequest     mem.Buffer
	getPartRequestInfo iec.PartInfo

	getPartRangeRequestsMtx sync.RWMutex
	getPartRangeRequests    map[iec.PartInfo]*preparedRangeRequest

	rangeViaGet bool
}

// CopyLocalECPartParentHeaderAndPayload implements [getsvc.GetECRequestTransport].
func (x *getECTransport) CopyLocalECPartParentHeaderAndPayload(ctx context.Context, storage *engine.StorageEngine, partInfo iec.PartInfo) (bool, uint64, uint64, uint64, error) {
	// TODO: handle request fields once and reuse
	addr := x.request.GetBody().GetAddress()
	cnr, err := cid.DecodeBytes(addr.GetContainerId().GetValue())
	if err != nil {
		return false, 0, 0, 0, fmt.Errorf("invalid container ID in request: %w", err)
	}
	id, err := oid.DecodeBytes(addr.GetObjectId().GetValue())
	if err != nil {
		return false, 0, 0, 0, fmt.Errorf("invalid object ID in request: %w", err)
	}

	logError := func(msg string, err error) {
		x.server.log.Warn(msg, zap.Stringer("container", cnr), zap.Stringer("parent", id),
			zap.Int("ruleIdx", partInfo.RuleIndex), zap.Int("partIdx", partInfo.Index), zap.Error(err))
	}

	hdrMemBuf, buf := getBufferForHeadResponse()

	prefixLen, stream, err := storage.ReadECPart(ctx, cnr, id, partInfo, buf)
	if err != nil {
		var splitErr *object.SplitInfoError
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.As(err, &splitErr) {
			return false, 0, 0, 0, err
		}
		logError("local storage failure (read EC part)", err)
		return false, 0, 0, 0, nil
	}

	defer stream.Close()

	_, _, partHdrf, err := iobject.GetNonPayloadFieldBounds(buf[:prefixLen])
	if err != nil {
		return false, 0, 0, 0, fmt.Errorf("parse first %d bytes of object protobuf: %w", prefixLen, err)
	}

	partHdrBuf := buf[partHdrf.ValueFrom:partHdrf.To]

	typ, err := iobject.GetTypeHeader(partHdrBuf)
	if err != nil {
		logError("invalid local object header (get type)", err)
		return false, 0, 0, 0, nil
	}
	if typ == object.TypeLink {
		return false, 0, 0, 0, getsvc.ErrLinker
	}

	partPldLen, err := iobject.GetPayloadLengthHeader(partHdrBuf)
	if err != nil {
		logError("invalid local object header (get payload length)", err)
		return false, 0, 0, 0, nil
	}

	parentIDf, parentSigf, parentHdrf, err := iobject.GetParentNonPayloadFieldBoundsHeader(partHdrBuf)
	if err != nil {
		logError("invalid local object header (get parent fields)", err)
		return false, 0, 0, 0, nil
	}

	parentPldLen, err := iobject.GetPayloadLengthHeader(partHdrBuf[parentHdrf.ValueFrom:parentHdrf.To])
	if err != nil {
		logError("invalid local object header (get payload length from parent header)", err)
		return false, 0, 0, 0, nil
	}

	var n int

	if !parentIDf.IsMissing() {
		// ID has same tag in header and split header
		n = copy(buf, partHdrBuf[parentIDf.From:parentIDf.To])
	}

	if !parentSigf.IsMissing() {
		partHdrBuf[parentSigf.From] = iprotobuf.TagBytes2
		n += copy(buf[n:], partHdrBuf[parentSigf.From:parentSigf.To])
	}

	if !partHdrf.IsMissing() {
		partHdrBuf[parentHdrf.From] = iprotobuf.TagBytes3
		n += copy(buf[n:], partHdrBuf[parentHdrf.From:parentHdrf.To])
	}

	err = x.server.copyGetStream(x.responseStream, hdrMemBuf, buf, prefixLen, n, stream, partHdrf.To, x.signResponses)
	if err != nil {
		var e copyReadError
		if !errors.As(err, &e) {
			return false, 0, 0, 0, err
		}
		logError("local storage stream failure (read EC part)", err)
		return true, parentPldLen, partPldLen, uint64(e.written), nil
	}

	return true, parentPldLen, partPldLen, partPldLen, nil
}

// CopyLocalECPartRange implements [getsvc.GetECRequestTransport].
func (x *getECTransport) CopyLocalECPartRange(ctx context.Context, storage *engine.StorageEngine, partInfo iec.PartInfo, off, ln uint64, controlCh <-chan bool) (uint64, error) {
	// TODO: handle request fields once and reuse
	addr := x.request.GetBody().GetAddress()
	cnr, err := cid.DecodeBytes(addr.GetContainerId().GetValue())
	if err != nil {
		return 0, fmt.Errorf("invalid container ID in request: %w", err)
	}
	id, err := oid.DecodeBytes(addr.GetObjectId().GetValue())
	if err != nil {
		return 0, fmt.Errorf("invalid object ID in request: %w", err)
	}

	logError := func(msg string, err error) {
		x.server.log.Warn(msg, zap.Stringer("container", cnr), zap.Stringer("parent", id),
			zap.Int("ruleIdx", partInfo.RuleIndex), zap.Int("partIdx", partInfo.Index), zap.Uint64("off", off), zap.Uint64("ln", ln), zap.Error(err))
	}

	hdrMemBuf, buf := getBufferForHeadResponse()
	stream, err := storage.ReadECPartRange(ctx, cnr, id, partInfo, off, ln, buf)
	hdrMemBuf.Free()
	if err != nil {
		if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) {
			return 0, err
		}
		logError("local storage failure (read EC part range)", err)
		return 0, nil
	}

	if stream == nil {
		return 0, nil
	}

	defer stream.Close()

	if controlCh != nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case abort := <-controlCh:
			if abort {
				return 0, getsvc.ErrAborted
			}
		}
	}

	err = x.server.copyRangeStream(x.responseStream, stream, x.signResponses, shiftPayloadChunkInGetResponseBuffer)
	if err != nil {
		var e copyReadError
		if !errors.As(err, &e) {
			return 0, err
		}
		logError("local storage stream failure (read EC part range)", err)
		return uint64(e.written), nil
	}

	return ln, nil
}

func (x *getECTransport) initGetPartRequest(partInfo iec.PartInfo) error {
	if x.getPartRequestInfo == partInfo && x.getPartRequest != nil {
		return nil
	}

	reqObj := x.request.GetBody().GetAddress()
	cnr := reqObj.GetContainerId().GetValue()
	parent := reqObj.GetObjectId().GetValue()

	reqMetaHdr := x.request.GetMetaHeader()
	sessionToken := reqMetaHdr.GetSessionTokenV2()
	sessionTokenV1 := reqMetaHdr.GetSessionToken()

	var err error
	x.getPartRequest, err = x.server.makeGetECPartRequest(cnr, parent, partInfo, sessionToken, sessionTokenV1)
	if err != nil {
		return fmt.Errorf("make GET request: %w", err)
	}

	x.getPartRequestInfo = partInfo

	return nil
}

// CopyRemoteECPartParentHeaderAndPayload implements [getsvc.GetECRequestTransport].
func (x *getECTransport) CopyRemoteECPartParentHeaderAndPayload(ctx context.Context, conn clientcore.MultiAddressClient, partInfo iec.PartInfo) (bool, uint64, uint64, uint64, error) {
	var copiedHdr bool
	var parentPldLen uint64
	var partPldLen uint64
	var copiedPartPld uint64

	err := conn.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		if !copiedHdr {
			if err := x.initGetPartRequest(partInfo); err != nil {
				return err
			}

			var err error
			copiedHdr, parentPldLen, partPldLen, copiedPartPld, err = x.copyRemotePart(ctx, conn)
			if err != nil {
				return err
			}

			if copiedHdr && copiedPartPld == partPldLen {
				return nil
			}

			return clientcore.ErrSkipConnection
		}

		copiedFromNode, err := x.copyRemotePartRange(ctx, conn, partInfo, copiedPartPld, parentPldLen-copiedPartPld, nil)
		if err != nil {
			return err
		}

		copiedPartPld += copiedFromNode
		if copiedPartPld > partPldLen {
			return fmt.Errorf("part payload overflow: full %d bytes, copied %d", partPldLen, copiedPartPld)
		}

		if copiedPartPld == partPldLen {
			return nil
		}

		return clientcore.ErrSkipConnection
	})
	if err != nil && !errors.Is(err, clientcore.ErrAllConnectionsSkipped) {
		return false, 0, 0, 0, err
	}

	return copiedHdr, parentPldLen, partPldLen, copiedPartPld, nil
}

func (x *getECTransport) copyRemotePart(ctx context.Context, conn *grpc.ClientConn) (bool, uint64, uint64, uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := callGet(ctx, conn, x.getPartRequest)
	if err != nil {
		if errors.Is(err, ctx.Err()) {
			return false, 0, 0, 0, err
		}
		// TODO: if error is due to incorrect request, error should be returned. How to catch this?
		x.server.log.Warn("GET object API failure (call)", zap.String("node", conn.Target()), zap.Error(err))
		return false, 0, 0, 0, nil
	}

	var copiedHdr bool
	var parentPldLen uint64
	var partPldLen uint64
	var copiedPartPldLen uint64

	var headWas bool
	for {
		var respBuf mem.BufferSlice
		if err = stream.RecvMsg(&respBuf); err != nil {
			if errors.Is(err, ctx.Err()) {
				return false, 0, 0, 0, err
			}
			if !errors.Is(err, io.EOF) {
				x.server.log.Warn("GET object API failure (receive message)", zap.String("node", conn.Target()), zap.Error(err))
			}
			break
		}

		code, body, err := handleResponseCodeAndBody(respBuf)
		if err != nil {
			respBuf.Free()
			return false, 0, 0, 0, err
		}

		if code == protostatus.ObjectNotFound {
			respBuf.Free()
			if headWas {
				return false, 0, 0, 0, errors.New("received object not found status after header")
			}
			return false, 0, 0, 0, nil
		}

		if code != protostatus.OK {
			if err = x.responseStream.SendMsg(respBuf); err != nil {
				return false, 0, 0, 0, fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
			}
			return false, 0, 0, 0, getsvc.ErrResponded
		}

		num, fld, err := handleGetResponseBodyOneof(&headWas, body)
		if err != nil {
			respBuf.Free()
			return false, 0, 0, 0, err
		}

		switch num {
		default:
			respBuf.Free()
			return false, 0, 0, 0, errors.New("none of the supported oneof fields are specified")
		case protoobject.FieldGetResponseBodyInit:
			var parentID, parentSig, parentHdr iprotobuf.BuffersSlice
			parentID, parentSig, parentHdr, parentPldLen, partPldLen, err = handleGetECPartResponseInit(fld)
			if err != nil {
				respBuf.Free()
				return false, 0, 0, 0, err
			}

			err = x.server.writeInitGetResponseBuffers(x.responseStream, parentID, parentSig, parentHdr, x.signResponses)
			respBuf.Free()
			if err != nil {
				return false, 0, 0, 0, err
			}

			copiedHdr = true
		case protoobject.FieldGetResponseBodyChunk:
			copiedPartPldLen += uint64(fld.Len())
			if copiedPartPldLen > partPldLen {
				respBuf.Free()
				return false, 0, 0, 0, fmt.Errorf("part payload overflow: full %d bytes, copied %d", partPldLen, copiedPartPldLen)
			}
			if copiedPartPldLen > parentPldLen {
				respBuf.Free()
				return false, 0, 0, 0, fmt.Errorf("parent payload overflow: full %d bytes, copied %d", parentPldLen, copiedPartPldLen)
			}

			if err = x.responseStream.SendMsg(respBuf); err != nil {
				return false, 0, 0, 0, fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
			}
		case protoobject.FieldGetResponseBodySplitInfo:
			err := handleSplitInfo(fld, true)
			respBuf.Free()
			return false, 0, 0, 0, err
		}
	}

	return copiedHdr, parentPldLen, partPldLen, copiedPartPldLen, nil
}

func (x *getECTransport) copyRemotePartRangeViaGet(ctx context.Context, conn *grpc.ClientConn, partInfo iec.PartInfo, off, ln uint64) (uint64, error) {
	if err := x.initGetPartRequest(partInfo); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := callGet(ctx, conn, x.getPartRequest)
	if err != nil {
		if errors.Is(err, ctx.Err()) {
			return 0, err
		}
		// TODO: if error is due to incorrect request, error should be returned. How to catch this?
		x.server.log.Warn("GET object API failure (call)", zap.String("node", conn.Target()), zap.Error(err))
		return 0, nil
	}

	var copied uint64

	var streamOff uint64
	var headWas bool
	for {
		var respBuf mem.BufferSlice
		if err = stream.RecvMsg(&respBuf); err != nil {
			if errors.Is(err, ctx.Err()) {
				return 0, err
			}
			fin := errors.Is(err, io.EOF)
			if fin && copied < ln {
				return 0, fmt.Errorf("received less bytes than requested: expected %d, got %d", ln, copied)
			}
			if !fin {
				x.server.log.Warn("GET object API failure (receive message)", zap.String("node", conn.Target()), zap.Error(err))
			}
			break
		}

		code, body, err := handleResponseCodeAndBody(respBuf)
		if err != nil {
			respBuf.Free()
			return 0, err
		}

		if code == protostatus.ObjectNotFound {
			respBuf.Free()
			if headWas {
				return 0, errors.New("received object not found status after header")
			}
			return 0, nil
		}

		if code != protostatus.OK {
			if err = x.responseStream.SendMsg(respBuf); err != nil {
				return 0, fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
			}
			return 0, getsvc.ErrResponded
		}

		num, fld, err := handleGetResponseBodyOneof(&headWas, body)
		if err != nil {
			respBuf.Free()
			return 0, err
		}

		switch num {
		default:
			respBuf.Free()
			return 0, errors.New("none of the supported oneof fields are specified")
		case protoobject.FieldGetResponseBodyInit:
			_, _, _, _, full, err := handleGetECPartResponseInit(fld)
			respBuf.Free()
			if err != nil {
				return 0, err
			}

			if ln != 0 {
				if off >= full || full-off < ln {
					return 0, apistatus.ErrObjectOutOfRange
				}
			}
		case protoobject.FieldGetResponseBodyChunk:
			var chunkFrom, chunkTo int
			chunkLen := fld.Len()

			if off > streamOff {
				diff := off - streamOff
				if uint64(chunkLen) <= diff { // already copied
					streamOff += uint64(chunkLen)
					break
				}
				chunkFrom = int(diff)
			}

			left := ln - copied

			if uint64(chunkLen-chunkFrom) > left {
				chunkTo = chunkFrom + int(left)
			} else {
				chunkTo = chunkLen
			}

			if chunkFrom > 0 || chunkTo < chunkLen {
				if chunkFrom > 0 {
					_, ok := fld.MoveNext(chunkFrom)
					if !ok {
						respBuf.Free()
						return 0, fmt.Errorf("%w while moving to offset=%d in chunk with length=%d", io.ErrUnexpectedEOF, chunkFrom, chunkLen)
					}
				}
				if chunkTo < chunkLen {
					var ok bool
					fld, ok = fld.MoveNext(chunkTo - chunkFrom)
					if !ok {
						respBuf.Free()
						return 0, fmt.Errorf("%w while moving to offset=%d in chunk with length=%d", io.ErrUnexpectedEOF, chunkTo-chunkFrom, chunkLen-chunkFrom)
					}
				}
				resp, err := x.server.makeGetChunkResponse(fld, x.signResponses)
				respBuf.Free()
				if err != nil {
					return 0, fmt.Errorf("make RANGE response: %w", err)
				}
				respBuf = mem.BufferSlice{resp}
			}

			if err = x.responseStream.SendMsg(respBuf); err != nil {
				return 0, fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
			}

			copied += uint64(chunkTo - chunkFrom)
			if copied == ln {
				return copied, nil
			}
		case protoobject.FieldGetResponseBodySplitInfo:
			err := handleSplitInfo(fld, true)
			respBuf.Free()
			return 0, err
		}
	}

	return copied, nil
}

func handleGetECPartResponseInit(buffers iprotobuf.BuffersSlice) (iprotobuf.BuffersSlice, iprotobuf.BuffersSlice, iprotobuf.BuffersSlice, uint64, uint64, error) {
	var parentID, parentSig, parentHdr iprotobuf.BuffersSlice
	var parentPldLen uint64
	var partPldLen uint64

	var opts protoscan.ScanMessageOptions
	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		if num != protoobject.FieldGetResponseBodyInitHeader {
			return protoscan.ErrContinue
		}

		var opts protoscan.ScanMessageOptions
		opts.InterceptUint64 = func(num protowire.Number, u uint64) error {
			if num == protoobject.FieldHeaderPayloadLength {
				partPldLen = u
			}
			return nil
		}
		opts.InterceptEnum = func(num protowire.Number, e int32) error {
			if num == protoobject.FieldHeaderObjectType && e == int32(protoobject.ObjectType_LINK) {
				return getsvc.ErrLinker
			}
			return nil
		}
		// TODO: consider returning parent header from the server (by specific flag for example).
		//  Now only payload len is used from the part header. It can be calculated from rule and parent len.
		opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
			if num != protoobject.FieldHeaderSplit {
				return protoscan.ErrContinue
			}

			var opts protoscan.ScanMessageOptions
			opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
				switch num { //nolint:exhaustive
				case protoobject.FieldHeaderSplitParentHeader:
					var opts protoscan.ScanMessageOptions
					opts.InterceptUint64 = func(num protowire.Number, u uint64) error {
						if num == protoobject.FieldHeaderPayloadLength {
							parentPldLen = u
						}
						return nil
					}
					if err := protoscan.ScanMessage(buffers, protoscan.ObjectHeaderScheme, opts); err != nil {
						return err
					}
					parentHdr = buffers
					return nil
				case protoobject.FieldHeaderSplitParent:
					parentID = buffers
				case protoobject.FieldHeaderSplitParentSignature:
					parentSig = buffers
				}
				return protoscan.ErrContinue
			}

			return protoscan.ScanMessage(buffers, protoscan.ObjectSplitHeaderScheme, opts)
		}

		return protoscan.ScanMessage(buffers, protoscan.ObjectHeaderScheme, opts)
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectGetResponseInitScheme, opts)
	if err != nil {
		return iprotobuf.BuffersSlice{}, iprotobuf.BuffersSlice{}, iprotobuf.BuffersSlice{}, 0, 0, err
	}

	return parentID, parentSig, parentHdr, parentPldLen, partPldLen, nil
}

func (x *getECTransport) CopyRemoteECPartRange(ctx context.Context, conn clientcore.MultiAddressClient, partInfo iec.PartInfo, off uint64, ln uint64, controlCh <-chan bool) (uint64, error) {
	var copiedPld uint64

	err := conn.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		copiedFromNode, err := x.copyRemotePartRange(ctx, conn, partInfo, off+copiedPld, ln-copiedPld, controlCh)
		if err != nil {
			return err
		}

		copiedPld += copiedFromNode
		if copiedPld > ln {
			return fmt.Errorf("received %d bytes while %d requested", copiedPld, ln)
		}

		if copiedPld == ln {
			return nil
		}

		return clientcore.ErrSkipConnection
	})
	if err != nil && !errors.Is(err, clientcore.ErrAllConnectionsSkipped) {
		return 0, err
	}

	return copiedPld, nil
}

func (x *getECTransport) copyRemotePartRange(ctx context.Context, conn *grpc.ClientConn, partInfo iec.PartInfo, off uint64, ln uint64, controlCh <-chan bool) (uint64, error) {
	if x.rangeViaGet {
		return x.copyRemotePartRangeViaGet(ctx, conn, partInfo, off, ln)
	}

	request, err := x.makeGetECPartRangeRequest(partInfo, off, ln)
	if err != nil {
		return 0, fmt.Errorf("make request: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := callRange(ctx, conn, request)
	if err != nil {
		if errors.Is(err, ctx.Err()) {
			return 0, err
		}
		x.server.log.Warn("RANGE object API failure (call)", zap.String("node", conn.Target()), zap.Error(err))
		return 0, nil
	}

	if controlCh != nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case abort := <-controlCh:
			if abort {
				return 0, getsvc.ErrAborted
			}
		}
	}

	var copiedPld uint64

	for first := true; ; first = false {
		var respBuf mem.BufferSlice
		if err = stream.RecvMsg(&respBuf); err != nil {
			if errors.Is(err, ctx.Err()) {
				return 0, err
			}
			fin := errors.Is(err, io.EOF)
			if fin && copiedPld < ln {
				return 0, fmt.Errorf("received less bytes than requested: expected %d, got %d", ln, copiedPld)
			}
			if !fin {
				x.server.log.Warn("RANGE object API failure (receive message)", zap.String("node", conn.Target()), zap.Error(err))
			}
			break
		}

		code, body, err := handleResponseCodeAndBody(respBuf)
		if err != nil {
			respBuf.Free()
			return 0, err
		}

		if code == protostatus.ObjectNotFound {
			respBuf.Free()
			if !first {
				return 0, errors.New("received object not found status in non-first message")
			}
			return 0, nil
		}

		// TODO: track https://github.com/nspcc-dev/neofs-node/issues/3547
		if code == protostatus.ObjectAccessDenied {
			respBuf.Free()
			if !first {
				return 0, errors.New("received access denied status in non-first message")
			}
			x.rangeViaGet = true
			return x.copyRemotePartRangeViaGet(ctx, conn, partInfo, off, ln)
		}

		if code != protostatus.OK {
			if err = x.responseStream.SendMsg(respBuf); err != nil {
				return 0, fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
			}
			return 0, getsvc.ErrResponded
		}

		num, fld, err := handleRangeResponseBodyOneof(body)
		if err != nil {
			respBuf.Free()
			return 0, err
		}

		switch num {
		default:
			respBuf.Free()
			return 0, errors.New("none of the supported oneof fields are specified")
		case protoobject.FieldRangeResponseBodyChunk:
			copiedPld += uint64(fld.Len())
			if copiedPld > ln {
				respBuf.Free()
				return 0, fmt.Errorf("received more bytes than requested: expected %d, got %d", ln, copiedPld)
			}

			// In fact, the only difference is in the 'body.chunk' field. With https://github.com/nspcc-dev/neofs-api/pull/389 it's gonna be GET only.
			getRespBuf, err := x.server.makeGetChunkResponse(fld, x.signResponses)
			respBuf.Free()
			if err != nil {
				return 0, fmt.Errorf("make GET chunk response: %w", err)
			}

			if err = x.responseStream.SendMsg(getRespBuf); err != nil {
				return 0, fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
			}
		case protoobject.FieldGetResponseBodySplitInfo:
			respBuf.Free()
			if !first {
				return 0, errors.New("received split info in non-first message")
			}
			return 0, errors.New("unexpected split info status response")
		}
	}

	return copiedPld, nil
}

func (s *Server) makeGetECPartRequest(cnr, parent []byte, partInfo iec.PartInfo, sessionToken *protosession.SessionTokenV2, sessionTokenV1 *protosession.SessionToken) (mem.Buffer, error) {
	ruleIdxStr := strconv.Itoa(partInfo.RuleIndex)
	partIdxStr := strconv.Itoa(partInfo.Index)

	ruleIdxHdrLen := calculateXHeaderLength(iec.AttributeRuleIdx, ruleIdxStr)
	partIdxHdrLen := calculateXHeaderLength(iec.AttributePartIdx, partIdxStr)

	sessionTokenLen := sessionToken.MarshaledSize()
	sessionTokenV1Len := sessionTokenV1.MarshaledSize()

	metaHdrLen := calculateGetECPartRequestMetaHeaderLength(ruleIdxHdrLen, partIdxHdrLen, sessionTokenLen, sessionTokenV1Len)

	reqLen := 1 + 1 + getByAddressRequestBodyLen + // first 1 for iprotobuf.TagBytes1
		1 + protowire.SizeBytes(metaHdrLen) + // 1 for iprotobuf.TagBytes2
		1 + 2 + requestVerificationHeaderECDSAWIthSHA512Len // 1 for iprotobuf.TagBytes3

	// TODO: try with sync.Pool
	buf := make([]byte, reqLen)

	n, err := s.writeGetECPartRequest(buf, cnr, parent, metaHdrLen, ruleIdxHdrLen, ruleIdxStr, partIdxHdrLen, partIdxStr,
		sessionTokenLen, sessionToken, sessionTokenV1Len, sessionTokenV1)
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("got wrong request length: expected %d, got %d", n, len(buf))
	}

	return mem.SliceBuffer(buf), nil
}

func (x *getECTransport) makeGetECPartRangeRequest(partInfo iec.PartInfo, off, ln uint64) (mem.Buffer, error) {
	x.getPartRangeRequestsMtx.RLock()
	req := x.getPartRangeRequests[partInfo]
	x.getPartRangeRequestsMtx.RUnlock()

	if req == nil {
		x.getPartRangeRequestsMtx.Lock()

		req = x.getPartRangeRequests[partInfo]
		if req == nil {
			if x.getPartRangeRequests == nil {
				x.getPartRangeRequests = make(map[iec.PartInfo]*preparedRangeRequest, 1)
			}
			req = new(preparedRangeRequest)
			x.getPartRangeRequests[partInfo] = req
		}

		x.getPartRangeRequestsMtx.Unlock()
	}

	if req.offset == off && req.length == ln && req.buffer != nil {
		return req.buffer, nil
	}

	reqObj := x.request.GetBody().GetAddress()
	cnr := reqObj.GetContainerId().GetValue()
	parent := reqObj.GetObjectId().GetValue()

	reqMetaHdr := x.request.GetMetaHeader()
	sessionToken := reqMetaHdr.GetSessionTokenV2()
	sessionTokenV1 := reqMetaHdr.GetSessionToken()

	reqBuf, err := x.server.makeGetECPartRangeRequest(cnr, parent, partInfo, off, ln, sessionToken, sessionTokenV1)
	if err != nil {
		// stream is closed by context cancellation
		return nil, err
	}

	req.buffer = reqBuf
	req.offset = off
	req.length = ln

	return reqBuf, nil
}

func (s *Server) makeGetECPartRangeRequest(cnr, parent []byte, partInfo iec.PartInfo, off, ln uint64, sessionToken *protosession.SessionTokenV2, sessionTokenV1 *protosession.SessionToken) (mem.Buffer, error) {
	ruleIdxStr := strconv.Itoa(partInfo.RuleIndex)
	partIdxStr := strconv.Itoa(partInfo.Index)

	ruleIdxHdrLen := calculateXHeaderLength(iec.AttributeRuleIdx, ruleIdxStr)
	partIdxHdrLen := calculateXHeaderLength(iec.AttributePartIdx, partIdxStr)

	sessionTokenLen := sessionToken.MarshaledSize()
	sessionTokenV1Len := sessionTokenV1.MarshaledSize()

	metaHdrLen := calculateGetECPartRequestMetaHeaderLength(ruleIdxHdrLen, partIdxHdrLen, sessionTokenLen, sessionTokenV1Len)

	var rngLen int
	if off != 0 {
		rngLen = 1 + protowire.SizeVarint(off) // 1 for iprotobuf.TagVarint1
	}
	if ln != 0 {
		rngLen += 1 + protowire.SizeVarint(ln) // 1 for iprotobuf.TagVarint2
	}

	bodyLen := getByAddressRequestBodyLen

	if rngLen != 0 {
		bodyLen += 1 + protowire.SizeBytes(rngLen) // 1 for iprotobuf.TagBytes2
	}

	reqLen := 1 + protowire.SizeBytes(bodyLen) + // 1 for iprotobuf.TagBytes1
		1 + protowire.SizeBytes(metaHdrLen) + // 1 for iprotobuf.TagBytes2
		1 + 2 + requestVerificationHeaderECDSAWIthSHA512Len // 1 for iprotobuf.TagBytes3

	// TODO: try with sync.Pool
	buf := make([]byte, reqLen)

	n, err := s.writeGetECPartRangeRequest(buf, bodyLen, cnr, parent, rngLen, off, ln,
		metaHdrLen, ruleIdxHdrLen, ruleIdxStr, partIdxHdrLen, partIdxStr,
		sessionTokenLen, sessionToken, sessionTokenV1Len, sessionTokenV1)
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("got wrong request length: expected %d, got %d", n, len(buf))
	}

	return mem.SliceBuffer(buf), nil
}

func (s *Server) writeGetECPartRequest(buf []byte, cnr []byte, parent []byte, metaHdrLen int, ruleIdxHdrLen int, ruleIdxHdr string, partIdxHdrLen int, partIdxHdr string,
	sessionTokenLen int, sessionToken *protosession.SessionTokenV2, sessionTokenV1Len int, sessionTokenV1 *protosession.SessionToken) (int, error) {
	// TODO: can be calculated once and reused
	originSig, err := neofsecdsa.Signer(s.signer).Sign(nil)
	if err != nil {
		return 0, fmt.Errorf("sign empty data: %w", err)
	}

	// body
	buf[0] = iprotobuf.TagBytes1
	buf[1] = getByAddressRequestBodyLen
	buf[2] = iprotobuf.TagBytes1 // address
	buf[3] = iprotobuf.ObjectAddressLength
	buf[4] = iprotobuf.TagBytes1 // CID
	buf[5] = iprotobuf.ContainerIDLength
	buf[6] = iprotobuf.TagBytes1 // value
	buf[7] = cid.Size
	copy(buf[8:], cnr)
	buf[40] = iprotobuf.TagBytes2 // OID
	buf[41] = iprotobuf.ObjectIDLength
	buf[42] = iprotobuf.TagBytes1 // value
	buf[43] = oid.Size
	copy(buf[44:], parent)

	bodySig, err := signECDSAWithSHA512(s.signer, buf[2:76])
	if err != nil {
		return 0, fmt.Errorf("sign body: %w", err)
	}

	// meta header
	buf[76] = iprotobuf.TagBytes2
	off := 77 + binary.PutUvarint(buf[77:], uint64(metaHdrLen))

	from := off

	off += copy(buf[off:], currentVersionResponseMetaHeader)
	off += writeRequestMetaXHeader(buf[off:], ruleIdxHdrLen, iec.AttributeRuleIdx, ruleIdxHdr)
	off += writeRequestMetaXHeader(buf[off:], partIdxHdrLen, iec.AttributePartIdx, partIdxHdr)

	if sessionTokenV1 != nil {
		off += writeStablyMarshalledField(buf[off:], iprotobuf.TagBytes5, sessionTokenV1Len, sessionTokenV1)
	}

	if sessionToken != nil {
		off += writeStablyMarshalledField(buf[off:], iprotobuf.TagBytes9, sessionTokenLen, sessionToken)
	}

	metaHdrSig, err := signECDSAWithSHA512(s.signer, buf[from:off])
	if err != nil {
		return 0, fmt.Errorf("sign meta header: %w", err)
	}

	// verification header
	off += writeRequestVerificationHeader(buf[off:], s.pubKeyBytes, bodySig, metaHdrSig, originSig)

	return off, nil
}

func (s *Server) writeGetECPartRangeRequest(buf []byte, bodyLen int, cnr []byte, parent []byte, rngLen int, off uint64, ln uint64,
	metaHdrLen int, ruleIdxHdrLen int, ruleIdxHdr string, partIdxHdrLen int, partIdxHdr string, sessionTokenLen int, sessionToken *protosession.SessionTokenV2,
	sessionTokenV1Len int, sessionTokenV1 *protosession.SessionToken) (int, error) {
	// TODO: can be calculated once and reused
	originSig, err := neofsecdsa.Signer(s.signer).Sign(nil)
	if err != nil {
		return 0, fmt.Errorf("sign empty data: %w", err)
	}

	// body
	buf[0] = iprotobuf.TagBytes1
	n := 1 + binary.PutUvarint(buf[1:], uint64(bodyLen))
	from := n
	buf[n] = iprotobuf.TagBytes1 // address
	n++
	buf[n] = iprotobuf.ObjectAddressLength
	n++
	buf[n] = iprotobuf.TagBytes1 // CID
	n++
	buf[n] = iprotobuf.ContainerIDLength
	n++
	buf[n] = iprotobuf.TagBytes1 // value
	n++
	buf[n] = cid.Size
	n++
	n += copy(buf[n:], cnr)
	buf[n] = iprotobuf.TagBytes2 // OID
	n++
	buf[n] = iprotobuf.ObjectIDLength
	n++
	buf[n] = iprotobuf.TagBytes1 // value
	n++
	buf[n] = oid.Size
	n++
	n += copy(buf[n:], parent)
	if rngLen != 0 {
		buf[n] = iprotobuf.TagBytes2 // range
		n++
		n += binary.PutUvarint(buf[n:], uint64(rngLen))
		if off != 0 {
			buf[n] = iprotobuf.TagVarint1
			n++
			n += binary.PutUvarint(buf[n:], off)
		}
		if ln != 0 {
			buf[n] = iprotobuf.TagVarint2
			n++
			n += binary.PutUvarint(buf[n:], ln)
		}
	}

	bodySig, err := signECDSAWithSHA512(s.signer, buf[from:n])
	if err != nil {
		return 0, fmt.Errorf("sign body: %w", err)
	}

	// meta header
	buf[n] = iprotobuf.TagBytes2
	n++
	n += binary.PutUvarint(buf[n:], uint64(metaHdrLen))

	from = n

	n += copy(buf[n:], currentVersionResponseMetaHeader)
	n += writeRequestMetaXHeader(buf[n:], ruleIdxHdrLen, iec.AttributeRuleIdx, ruleIdxHdr)
	n += writeRequestMetaXHeader(buf[n:], partIdxHdrLen, iec.AttributePartIdx, partIdxHdr)

	if sessionTokenV1 != nil {
		n += writeStablyMarshalledField(buf[n:], iprotobuf.TagBytes5, sessionTokenV1Len, sessionTokenV1)
	}

	if sessionToken != nil {
		n += writeStablyMarshalledField(buf[n:], iprotobuf.TagBytes9, sessionTokenLen, sessionToken)
	}

	metaHdrSig, err := signECDSAWithSHA512(s.signer, buf[from:n])
	if err != nil {
		return 0, fmt.Errorf("sign meta header: %w", err)
	}

	// verification header
	n += writeRequestVerificationHeader(buf[n:], s.pubKeyBytes, bodySig, metaHdrSig, originSig)

	return n, nil
}

func (s *Server) writeInitGetResponseBuffers(respStream grpc.ServerStream, id, sig, hdr iprotobuf.BuffersSlice, signResponse bool) error {
	idLen := id.Len()
	sigLen := sig.Len()
	hdrLen := hdr.Len()

	initFldLen := calculateInitGetResponseFieldLength(idLen, sigLen, hdrLen)

	bodyLen := 1 + protowire.SizeBytes(initFldLen) // 1 for iprotobuf.TagBytes1

	respLen := 1 + protowire.SizeBytes(bodyLen) // 1 for iprotobuf.TagBytes1

	if signResponse {
		respLen += 1 + protowire.SizeBytes(requestVerificationHeaderECDSAWIthSHA512Len) // 1 for iprotobuf.TagBytes3
	}

	var respBuf mem.Buffer
	var buf mem.SliceBuffer
	if respLen <= headResponseBufferLen {
		hb, _ := getBufferForHeadResponse()
		respBuf = hb
		buf = hb.SliceBuffer
	} else {
		buf = make(mem.SliceBuffer, respLen)
		respBuf = buf
	}

	// body
	buf[0] = iprotobuf.TagBytes1
	off := 1 + binary.PutUvarint(buf[1:], uint64(bodyLen))
	bodyFrom := off
	// init
	buf[off] = iprotobuf.TagBytes1
	off++
	off += binary.PutUvarint(buf[off:], uint64(initFldLen))
	// id
	buf[off] = iprotobuf.TagBytes1
	off++
	off += binary.PutUvarint(buf[off:], uint64(idLen))
	off += id.CopyTo(buf[off:])
	// signature
	buf[off] = iprotobuf.TagBytes2
	off++
	off += binary.PutUvarint(buf[off:], uint64(sigLen))
	off += sig.CopyTo(buf[off:])
	// header
	buf[off] = iprotobuf.TagBytes3
	off++
	off += binary.PutUvarint(buf[off:], uint64(hdrLen))
	off += hdr.CopyTo(buf[off:])

	if signResponse {
		n, err := s.signResponse(buf[off:], buf[bodyFrom:off], nil)
		if err != nil {
			respBuf.Free()
			return fmt.Errorf("sign response: %w", err)
		}
		off += n
	}

	if respLen <= headResponseBufferLen {
		respBuf.(*iprotobuf.MemBuffer).SetBounds(0, off)
	}

	return respStream.SendMsg(respBuf)
}

func (s *Server) makeGetChunkResponse(chunk iprotobuf.BuffersSlice, sign bool) (mem.Buffer, error) {
	chunkLen := chunk.Len()

	bodyLen := 1 + protowire.SizeBytes(chunkLen) // 1 for iprotobuf.TagBytes1

	respLen := 1 + protowire.SizeBytes(bodyLen) // 1 for iprotobuf.TagBytes1

	if sign {
		respLen += 1 + protowire.SizeBytes(requestVerificationHeaderECDSAWIthSHA512Len) // 1 for iprotobuf.TagBytes3
	}

	var respBuf mem.Buffer
	var buf mem.SliceBuffer
	if respLen <= maxGetResponseChunkLen {
		hb, _ := getBufferForChunkGetResponse()
		respBuf = hb
		buf = hb.SliceBuffer
	} else {
		buf = make(mem.SliceBuffer, respLen)
		respBuf = buf
	}

	// body
	buf[0] = iprotobuf.TagBytes1
	off := 1 + binary.PutUvarint(buf[1:], uint64(bodyLen))
	bodyFrom := off
	// chunk
	buf[off] = iprotobuf.TagBytes2
	off++
	off += binary.PutUvarint(buf[off:], uint64(chunkLen))
	off += chunk.CopyTo(buf[off:])

	if sign {
		n, err := s.signResponse(buf[off:], buf[bodyFrom:off], nil)
		if err != nil {
			return nil, fmt.Errorf("sign response: %w", err)
		}
		off += n
	}

	if respLen <= maxGetResponseChunkLen {
		respBuf.(*iprotobuf.MemBuffer).SetBounds(0, off)
	}

	return respBuf, nil
}

func calculateGetECPartRequestMetaHeaderLength(ruleIdxHdrLen, partIdxHdrLen, sessionTokenLen, sessionTokenV1Len int) int {
	metaHdrLen := len(currentVersionResponseMetaHeader) +
		1 + protowire.SizeBytes(ruleIdxHdrLen) + // 1 for iprotobuf.TagBytes4
		1 + protowire.SizeBytes(partIdxHdrLen) // 1 for iprotobuf.TagBytes4

	if sessionTokenLen > 0 {
		metaHdrLen += 1 + protowire.SizeBytes(sessionTokenLen) // 1 for iprotobuf.TagBytes9
	}

	if sessionTokenV1Len > 0 {
		metaHdrLen += 1 + protowire.SizeBytes(sessionTokenV1Len) // 1 for iprotobuf.TagBytes5
	}

	return metaHdrLen
}

func calculateInitGetResponseFieldLength(idLen, sigLen, hdrLen int) int {
	return 1 + protowire.SizeBytes(idLen) + // 1 for iprotobuf.TagBytes1
		1 + protowire.SizeBytes(sigLen) + // 1 for iprotobuf.TagBytes2
		1 + protowire.SizeBytes(hdrLen) // 1 for iprotobuf.TagBytes3
}
