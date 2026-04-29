package object

import (
	"context"
	"errors"
	"fmt"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

// returns:
//   - response buffer and object header protobuf without an error on OK
//   - (nil, nil, [apistatus.ErrObjectNotFound]) on 404 status
//   - (buffered response, nil, nil) on other API statuses
//   - (nil, nil, err) on any transport err
func getHeaderFromRemoteNode(ctx context.Context, conn *grpc.ClientConn, req *protoobject.HeadRequest, reqOID oid.ID) (mem.BufferSlice, iprotobuf.BuffersSlice, error) {
	var respBuf mem.BufferSlice

	err := conn.Invoke(ctx, protoobject.ObjectService_Head_FullMethodName, req, &respBuf,
		grpc.StaticMethod(),
		grpc.ForceCodecV2(iprotobuf.BufferedCodec{}),
	)
	if err != nil {
		return nil, iprotobuf.BuffersSlice{}, fmt.Errorf("sending the request failed: %w", err)
	}

	hdrBuffers, err := handleBufferedHeadResponse(respBuf, reqOID)
	if err != nil {
		respBuf.Free()
		return nil, iprotobuf.BuffersSlice{}, fmt.Errorf("handle response: %w", err)
	}

	return respBuf, hdrBuffers, nil
}

func handleBufferedHeadResponse(buffers mem.BufferSlice, reqOID oid.ID) (iprotobuf.BuffersSlice, error) {
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

	err := protoscan.ScanMessage(iprotobuf.NewBuffersSlice(buffers), protoscan.ResponseScheme, opts)
	if err != nil {
		return iprotobuf.BuffersSlice{}, err
	}

	if code == protostatus.ObjectNotFound {
		return iprotobuf.BuffersSlice{}, apistatus.ErrObjectNotFound
	}

	if code != protostatus.OK {
		return iprotobuf.BuffersSlice{}, nil
	}

	hdrBuffers, err := handleBufferedHeadResponseBody(body, reqOID)
	if err != nil {
		return iprotobuf.BuffersSlice{}, fmt.Errorf("handle body: %w", err)
	}

	return hdrBuffers, nil
}

func handleBufferedHeadResponseBody(buffers iprotobuf.BuffersSlice, reqOID oid.ID) (iprotobuf.BuffersSlice, error) {
	var oneofNum protowire.Number
	var oneofFld iprotobuf.BuffersSlice
	var opts protoscan.ScanMessageOptions

	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		switch num {
		default:
			return protoscan.ErrContinue
		case protoobject.FieldHeadResponseBodyHeader,
			protoobject.FieldHeadResponseBodyShortHeader,
			protoobject.FieldHeadResponseBodySplitInfo:
			oneofNum, oneofFld = num, buffers
			return nil
		}
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectHeadResponseBodyScheme, opts)
	if err != nil {
		return iprotobuf.BuffersSlice{}, err
	}

	switch oneofNum {
	default:
		return iprotobuf.BuffersSlice{}, errors.New("none of the supported oneof fields are specified")
	case protoobject.FieldHeadResponseBodyHeader:
		hdrBuffers, err := handleBufferedHeaderWithSignature(oneofFld, reqOID)
		if err != nil {
			return iprotobuf.BuffersSlice{}, fmt.Errorf("handle header with signature field: %w", err)
		}
		return hdrBuffers, nil
	case protoobject.FieldHeadResponseBodyShortHeader:
		return iprotobuf.BuffersSlice{}, errors.New("unsupported short header")
	case protoobject.FieldHeadResponseBodySplitInfo:
		err := protoscan.ScanMessage(oneofFld, protoscan.ObjectSplitInfoScheme, protoscan.ScanMessageOptions{})
		if err != nil {
			return iprotobuf.BuffersSlice{}, fmt.Errorf("handle split info field: %w", err)
		}
		return iprotobuf.BuffersSlice{}, nil
	}
}

func handleBufferedHeaderWithSignature(buffers iprotobuf.BuffersSlice, reqOID oid.ID) (iprotobuf.BuffersSlice, error) {
	var withHdr bool
	var hdrBuffers iprotobuf.BuffersSlice
	var hdrOrdered bool
	var withSig bool
	var opts protoscan.ScanMessageOptions

	opts.InterceptNested = func(num protowire.Number, buffers iprotobuf.BuffersSlice) error {
		if num != protoobject.FieldHeaderWithSignatureHeader {
			if num == protoobject.FieldHeaderWithSignatureSignature {
				withSig = true
			}
			return protoscan.ErrContinue
		}

		var err error
		hdrOrdered, err = protoscan.ScanMessageOrdered(buffers, protoscan.ObjectHeaderScheme, protoscan.ScanMessageOrderedOptions{})
		if err != nil {
			return fmt.Errorf("handle header with signature field: %w", err)
		}

		hdrBuffers = buffers
		withHdr = true
		return nil
	}

	err := protoscan.ScanMessage(buffers, protoscan.ObjectHeaderWithSignatureScheme, opts)
	if err != nil {
		return hdrBuffers, err
	}

	if !withHdr {
		return hdrBuffers, errors.New("missing header")
	}
	if !withSig {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return hdrBuffers, errors.New("missing signature")
	}

	return hdrBuffers, checkHeaderProtobufAgainstID(hdrBuffers, reqOID, hdrOrdered)
}
