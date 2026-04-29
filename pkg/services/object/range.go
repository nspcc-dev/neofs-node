package object

import (
	"context"
	"errors"
	"fmt"
	"io"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

type rangeStreamProgress struct {
	empty       bool
	readPayload int
}

// returns:
//   - nil on completed payload transmission
//   - [object.SplitInfoError]/nil on split info response and unset/set raw flag in request
//   - [apistatus.ErrObjectNotFound] on 404 status
//   - nil on other API statuses
//   - any other transport/protocol error otherwise
func (s *rangeStream) continueWithConn(ctx context.Context, conn *grpc.ClientConn) error {
	stream, err := conn.NewStream(ctx, &protoobject.ObjectService_ServiceDesc.Streams[3], protoobject.ObjectService_GetRange_FullMethodName,
		grpc.StaticMethod(),
		grpc.ForceCodecV2(iprotobuf.BufferedCodec{}),
	)
	if err != nil {
		return fmt.Errorf("stream opening failed: %w", err)
	}
	if err = stream.SendMsg(s.req); err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("close send: %w", err)
	}

	var prog rangeStreamProgress
	for {
		var respBuf mem.BufferSlice
		if err = stream.RecvMsg(&respBuf); err != nil {
			if errors.Is(err, io.EOF) {
				if prog.empty {
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			return fmt.Errorf("reading the response failed: %w", err)
		}

		fin, sent, err := s.handleResponse(&prog, respBuf)
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

func (s *rangeStream) handleResponse(streamProg *rangeStreamProgress, respBuf mem.BufferSlice) (bool, bool, error) {
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
		return true, true, s.base.SendMsg(respBuf)
	}

	sent, err := s.handleResponseBody(streamProg, respBuf, body)
	if err != nil {
		return false, sent, fmt.Errorf("handle body: %w", err)
	}

	return false, sent, nil
}

func (s *rangeStream) handleResponseBody(streamProg *rangeStreamProgress, respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
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
		switch num {
		default:
			return protoscan.ErrContinue
		case protoobject.FieldGetResponseBodySplitInfo:
			oneofNum, oneofFld = num, buffers
			return nil
		}
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectGetRangeResponseBodyScheme, opts)
	if err != nil {
		return false, err
	}

	switch oneofNum {
	default:
		return false, errors.New("none of the supported oneof fields are specified")
	case protoobject.FieldRangeResponseBodyChunk:
		return s.handleChunkResponse(streamProg, respBuf, oneofFld)
	case protoobject.FieldRangeResponseBodySplitInfo:
		return s.handleSplitInfo(respBuf, oneofFld)
	}
}

func (s *rangeStream) handleChunkResponse(streamProg *rangeStreamProgress, respBuf mem.BufferSlice, chunkBuffers iprotobuf.BuffersSlice) (bool, error) {
	chunkLen := chunkBuffers.Len()

	from, to := chunkBoundsToSend(s.respondedPayload, streamProg.readPayload, chunkLen)
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

	return s.srv.sendChunkResponse(s.base, respBuf, chunkBuffers, to-from, chunkLen,
		s.signResponse, iprotobuf.TagBytes1, &streamProg.readPayload, &s.respondedPayload, shiftPayloadChunkInRangeResponseBuffer)
}

func (s *rangeStream) handleSplitInfo(respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
	return handleSplitInfo(s.req.GetBody().GetRaw(), s.base, respBuf, buffers)
}
