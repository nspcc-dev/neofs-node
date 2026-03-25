package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

type getStreamProgress struct {
	headWas     bool
	readPayload int
}

// returns:
//   - nil on completed object transmission
//   - [object.SplitInfoError]/nil on split info response and unset/set raw flag in request
//   - [apistatus.ErrObjectNotFound] on 404 status
//   - nil on other API statuses
//   - any other transport/protocol error otherwise
func (x *getProxyContext) continueWithConn(ctx context.Context, conn *grpc.ClientConn) error {
	stream, err := conn.NewStream(ctx, &protoobject.ObjectService_ServiceDesc.Streams[0], protoobject.ObjectService_Get_FullMethodName,
		grpc.StaticMethod(),
		grpc.ForceCodecV2(iprotobuf.BufferedCodec{}),
	)
	if err != nil {
		return fmt.Errorf("stream opening failed: %w", err)
	}
	if err = stream.SendMsg(x.req); err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("close send: %w", err)
	}

	// TODO: dont forget to free buffers when responses ignored

	var prog getStreamProgress
	for {
		var respBuf mem.BufferSlice
		if err = stream.RecvMsg(&respBuf); err != nil {
			if errors.Is(err, io.EOF) {
				if !prog.headWas {
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			return fmt.Errorf("reading the response failed: %w", err)
		}

		fin, sent, err := x.handleGetResponse(&prog, respBuf)
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

func (x *getProxyContext) handleGetResponse(streamProg *getStreamProgress, respBuf mem.BufferSlice) (bool, bool, error) {
	var code uint32
	var body iprotobuf.BuffersSlice
	var opts protoscan.ScanMessageOptions

	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		switch num {
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
		return protoscan.ErrContinue
	}

	err := protoscan.ScanMessage(iprotobuf.NewBuffersSlice(respBuf), protoscan.ResponseScheme, opts)
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

	sent, err := x.handleResponseBody(streamProg, respBuf, body)
	if err != nil {
		return false, sent, fmt.Errorf("handle body: %w", err)
	}

	return false, sent, nil
}

func (x *getProxyContext) handleResponseBody(streamProg *getStreamProgress, respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
	var oneofNum protowire.Number
	var oneofFld iprotobuf.BuffersSlice
	var opts protoscan.ScanMessageOptions

	opts.InterceptBytes = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		if num == protoobject.FieldGetResponseBodyChunk {
			if !streamProg.headWas {
				return errors.New("incorrect message sequence")
			}
			oneofNum, oneofFld = num, buffers
		}
		return nil
	}
	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		switch num {
		case protoobject.FieldGetResponseBodyInit:
			if streamProg.headWas {
				return errors.New("incorrect message sequence")
			}
			streamProg.headWas = true
			oneofNum, oneofFld = num, buffers
			return nil
		case protoobject.FieldGetResponseBodySplitInfo:
			oneofNum, oneofFld = num, buffers
			return nil
		}
		return protoscan.ErrContinue
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectGetResponseBodyScheme, opts)
	if err != nil {
		return false, err
	}

	switch oneofNum {
	default:
		return false, errors.New("none of the supported oneof fields are specified")
	case protoobject.FieldGetResponseBodyInit:
		return x.handleInitResponse(respBuf, oneofFld)
	case protoobject.FieldGetResponseBodyChunk:
		return x.handleChunkResponse(streamProg, respBuf, oneofFld)
	case protoobject.FieldGetResponseBodySplitInfo:
		return x.handleSplitInfo(respBuf, oneofFld)
	}
}

func (x *getProxyContext) handleInitResponse(respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
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
			err = x.respStream.srv.aclChecker.CheckEACL(hdr, x.respStream.reqInfo)
			if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
				err = eACLErr(x.respStream.reqInfo, err)
			}
		}
		if err == nil {
			err = x.respStream.base.SendMsg(respBuf)
			sent = true
		}
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

	_, err := chunkBuffers.MoveNext(from)
	if err != nil {
		return false, fmt.Errorf("seek chunk left bound in response buffers: %w", err)
	}

	chunkBuffers, err = chunkBuffers.MoveNext(to - from)
	if err != nil {
		return false, fmt.Errorf("seek chunk right bound in response buffers: %w", err)
	}

	if x.payloadHashGot == nil {
		x.payloadHashGot = sha256.New()
	}
	chunkBuffers.HashTo(x.payloadHashGot)

	respChunkLen := to - from

	if uint64(x.respondedPayload+respChunkLen) == x.payloadLenCheck {
		if !bytes.Equal(x.payloadHashGot.Sum(nil), x.payloadHashCheck) { // not merged via && for readability
			return false, errors.New("received payload mismatches checksum from header")
		}
	}

	remoteSent := respChunkLen == chunkLen
	if !remoteSent {
		if respChunkLen <= maxGetResponseChunkLen {
			localRespBuf, _ := getBufferForChunkGetResponse()

			chunkBuffers.CopyTo(localRespBuf.SliceBuffer[maxChunkOffsetInGetResponse:])

			bodyf := shiftPayloadChunkInGetResponseBuffer(localRespBuf.SliceBuffer, maxChunkOffsetInGetResponse, respChunkLen)

			if x.respStream.signResponse {
				n, err := x.respStream.srv.signResponse(localRespBuf.SliceBuffer[bodyf.To:], localRespBuf.SliceBuffer[bodyf.ValueFrom:bodyf.To], nil)
				if err != nil {
					return false, fmt.Errorf("sign chunk response: %w", err)
				}
				bodyf.To += n
			}

			localRespBuf.SetBounds(bodyf.From, bodyf.To)
			respBuf = mem.BufferSlice{localRespBuf}
		} else {
			// TODO: in this case we could make respBuf = mem.BufferSlice{prefix, chunkBuffers},
			//  but then we'd have to provide mem.Buffer from iprotobuf.BuffersSlice
			bodyFldLen := 1 + protowire.SizeBytes(respChunkLen)
			fullLen := 1 + protowire.SizeBytes(bodyFldLen)
			if x.respStream.signResponse {
				fullLen += maxResponseVerificationHeaderLen
			}

			b := make(mem.SliceBuffer, fullLen)
			b[0] = iprotobuf.TagBytes1 // body field
			off := 1 + binary.PutUvarint(b[1:], uint64(bodyFldLen))
			b[off] = iprotobuf.TagBytes2 // chunk field
			off += 1 + binary.PutUvarint(b[off+1:], uint64(respChunkLen))
			off += chunkBuffers.CopyTo(b[off:])
			if x.respStream.signResponse {
				n, err := x.respStream.srv.signResponse(b[off:], b[:off], nil)
				if err != nil {
					return false, fmt.Errorf("sign chunk response: %w", err)
				}
				b = b[:off+n]
			}

			respBuf = mem.BufferSlice{b}
		}
	}

	if err := x.respStream.base.SendMsg(respBuf); err != nil {
		return remoteSent, err
	}

	streamProg.readPayload += chunkLen
	x.respondedPayload += to - from

	return remoteSent, nil
}

func (x *getProxyContext) handleSplitInfo(respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
	var si object.SplitInfo
	var opts protoscan.ScanMessageOptions

	compose := !x.req.GetBody().GetRaw()
	if compose {
		opts.InterceptBytes = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
			if num == protoobject.FieldSplitInfoSplitID {
				id := object.NewSplitIDFromV2(buffers.ReadOnlyData())
				if id == nil {
					return errors.New("invalid split ID")
				}
				si.SetSplitID(id)
			}
			return nil
		}
		opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
			if num != protoobject.FieldSplitInfoLastPart && num != protoobject.FieldSplitInfoLink && num != protoobject.FieldSplitInfoFirstPart {
				return protoscan.ErrContinue
			}

			var opts protoscan.ScanMessageOptions
			opts.InterceptBytes = func(num2 protowire.Number, buffers iprotobuf.BuffersSlice) error {
				if num2 == protorefs.FieldObjectIDValue {
					switch num {
					case protoobject.FieldSplitInfoLastPart:
						si.SetLastPart(oid.ID(buffers.ReadOnlyData()))
					case protoobject.FieldSplitInfoLink:
						si.SetLink(oid.ID(buffers.ReadOnlyData()))
					case protoobject.FieldSplitInfoFirstPart:
						si.SetLastPart(oid.ID(buffers.ReadOnlyData()))
					}
				}
				return nil
			}
			return protoscan.ScanMessage(buffers, protoscan.ObjectIDScheme, opts)
		}
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectSplitInfoScheme, opts)
	if err != nil {
		return false, fmt.Errorf("handle split info field: %w", err)
	}

	if compose {
		return false, object.NewSplitInfoError(&si)
	}

	return true, x.respStream.base.SendMsg(respBuf)
}
