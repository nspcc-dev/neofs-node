package object

import (
	"encoding/binary"
	"errors"
	"fmt"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

func handleSplitInfo(raw bool, respStream grpc.ServerStream, respBuf mem.BufferSlice, buffers iprotobuf.BuffersSlice) (bool, error) {
	var si object.SplitInfo
	var opts protoscan.ScanMessageOptions

	if !raw {
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
					switch num { //nolint:exhaustive
					case protoobject.FieldSplitInfoLastPart:
						si.SetLastPart(oid.ID(buffers.ReadOnlyData()))
					case protoobject.FieldSplitInfoLink:
						si.SetLink(oid.ID(buffers.ReadOnlyData()))
					case protoobject.FieldSplitInfoFirstPart:
						si.SetFirstPart(oid.ID(buffers.ReadOnlyData()))
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

	if !raw {
		return false, object.NewSplitInfoError(&si)
	}

	return true, respStream.SendMsg(respBuf)
}

func (s *Server) sendChunkResponse(respStream grpc.ServerStream, respBuf mem.BufferSlice, chunkBuffers iprotobuf.BuffersSlice,
	respChunkLen, chunkLen int, signResponse bool, chunkFldTag byte, readStream, responded *int, shiftFunc func([]byte, int, int) iprotobuf.FieldBounds) (bool, error) {
	remoteSent := respChunkLen == chunkLen
	if !remoteSent {
		if respChunkLen <= maxGetResponseChunkLen {
			localRespBuf, _ := getBufferForChunkGetResponse()

			chunkBuffers.CopyTo(localRespBuf.SliceBuffer[maxChunkOffsetInGetResponse:])

			bodyf := shiftFunc(localRespBuf.SliceBuffer, maxChunkOffsetInGetResponse, respChunkLen)

			if signResponse {
				n, err := s.signResponse(localRespBuf.SliceBuffer[bodyf.To:], localRespBuf.SliceBuffer[bodyf.ValueFrom:bodyf.To], nil)
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
			if signResponse {
				fullLen += maxResponseVerificationHeaderLen
			}

			b := make(mem.SliceBuffer, fullLen)
			b[0] = iprotobuf.TagBytes1 // body field
			off := 1 + binary.PutUvarint(b[1:], uint64(bodyFldLen))
			b[off] = chunkFldTag
			off += 1 + binary.PutUvarint(b[off+1:], uint64(respChunkLen))
			off += chunkBuffers.CopyTo(b[off:])
			if signResponse {
				n, err := s.signResponse(b[off:], b[:off], nil)
				if err != nil {
					return false, fmt.Errorf("sign chunk response: %w", err)
				}
				b = b[:off+n]
			}

			respBuf = mem.BufferSlice{b}
		}
	}

	if err := respStream.SendMsg(respBuf); err != nil {
		return remoteSent, err
	}

	*readStream += chunkLen
	*responded += respChunkLen

	return remoteSent, nil
}
