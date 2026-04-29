package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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
		default:
			return protoscan.ErrContinue
		case protoobject.FieldGetResponseBodyInit:
			if streamProg.headWas {
				return errors.New("incorrect message sequence")
			}
			streamProg.headWas = true
			oneofNum, oneofFld = num, buffers
			return nil
		case protoobject.FieldGetResponseBodySplitInfo:
			streamProg.headWas = true
			oneofNum, oneofFld = num, buffers
			return nil
		}
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
			err = x.respStream.srv.aclChecker.CheckEACL(hdr.ReadOnlyData(), x.respStream.reqInfo)
			if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
				err = eACLErr(x.respStream.reqInfo, err)
				return
			}
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
	return handleSplitInfo(x.req.GetBody().GetRaw(), x.respStream.base, respBuf, buffers)
}
