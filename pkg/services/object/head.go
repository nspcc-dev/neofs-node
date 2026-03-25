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
//   - (nil, nil, [object.SplitInfoError]) on OK with corresponding body field
//   - (nil, nil, [apistatus.ErrObjectNotFound]) on 404 status
//   - (respBuf, nil, nil) on other API statuses
//   - (nil, nil, err) on any transport err
func getHeaderFromRemoteNode(ctx context.Context, conn *grpc.ClientConn, req *protoobject.HeadRequest, reqOID oid.ID) (mem.BufferSlice, []byte, error) {
	var respBuf mem.BufferSlice

	err := conn.Invoke(ctx, protoobject.ObjectService_Head_FullMethodName, req, &respBuf,
		grpc.StaticMethod(),
		grpc.ForceCodecV2(iprotobuf.BufferedCodec{}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("sending the request failed: %w", err)
	}

	hdr, err := handleHeadResponse(respBuf, reqOID)
	if err != nil {
		respBuf.Free()
		return nil, nil, fmt.Errorf("handle response: %w", err)
	}

	return respBuf, hdr, nil
}

func handleHeadResponse(respBuf mem.BufferSlice, reqOID oid.ID) ([]byte, error) {
	var buf []byte
	if len(respBuf) == 1 {
		buf = respBuf[0].ReadOnlyData()
	} else {
		// TODO: consider optimization
		// This concats all buffers. We could iterate over each buffer io.MultiReader-like,
		// but that's not trivial. Anyway, HEAD responses fit into single buffer mostly.
		buf = respBuf.Materialize()
	}

	var code uint32
	var body []byte
	var opts protoscan.ScanMessageOptions

	opts.InterceptNested = func(num protowire.Number, fld []byte) error {
		switch num {
		case iprotobuf.FieldResponseBody:
			body = fld
			return nil
		case iprotobuf.FieldResponseMetaHeader:
			var err error
			code, err = getStatusCodeFromResponseMetaHeader(fld)
			if err != nil {
				return fmt.Errorf("handle meta header: %w", err)
			}
			return nil
		}
		return protoscan.ErrContinue
	}

	err := protoscan.ScanMessage(buf, protoscan.ResponseScheme, opts)
	if err != nil {
		return nil, err
	}

	if code == protostatus.ObjectNotFound {
		return nil, apistatus.ErrObjectNotFound
	}

	if code != protostatus.OK {
		return nil, nil
	}

	// TODO: forbid body if code != OK?

	hdr, err := handleHeadResponseBody(body, reqOID)
	if err != nil {
		return nil, fmt.Errorf("handle body: %w", err)
	}

	return hdr, nil
}

func handleHeadResponseBody(buf []byte, reqOID oid.ID) ([]byte, error) {
	var oneofNum protowire.Number
	var oneofFld []byte
	var opts protoscan.ScanMessageOptions

	opts.InterceptNested = func(num protowire.Number, fld []byte) error {
		switch num {
		case protoobject.FieldHeadResponseBodyHeader,
			protoobject.FieldHeadResponseBodyShortHeader,
			protoobject.FieldHeadResponseBodySplitInfo:
			oneofNum, oneofFld = num, fld
			return nil
		}
		return protoscan.ErrContinue
	}

	err := protoscan.ScanMessage(buf, protoscan.ObjectHeadResponseBodyScheme, opts)
	if err != nil {
		return nil, err
	}

	switch oneofNum {
	default:
		return nil, errors.New("none of the supported oneof fields are specified")
	case protoobject.FieldHeadResponseBodyHeader:
		hdr, err := handleHeaderWithSignature(oneofFld, reqOID)
		if err != nil {
			return nil, fmt.Errorf("handle header with signature field: %w", err)
		}
		return hdr, nil
	case protoobject.FieldHeadResponseBodyShortHeader:
		return nil, errors.New("unsupported short header")
	case protoobject.FieldHeadResponseBodySplitInfo:
		err := protoscan.ScanMessage(oneofFld, protoscan.ObjectSplitInfoScheme, protoscan.ScanMessageOptions{})
		if err != nil {
			return nil, fmt.Errorf("handle split info field: %w", err)
		}
		return nil, nil
	}
}

func handleHeaderWithSignature(buf []byte, reqOID oid.ID) ([]byte, error) {
	var hdr []byte
	var hdrOrdered bool
	var withSig bool
	var opts protoscan.ScanMessageOptions

	opts.InterceptNested = func(num protowire.Number, fld []byte) error {
		if num != protoobject.FieldHeaderWithSignatureHeader {
			if num == protoobject.FieldHeaderWithSignatureSignature {
				withSig = true
			}
			return protoscan.ErrContinue
		}

		var err error
		hdrOrdered, err = protoscan.ScanMessageOrdered(fld, protoscan.ObjectHeaderScheme, protoscan.ScanMessageOrderedOptions{})
		if err != nil {
			return fmt.Errorf("handle header with signature field: %w", err)
		}

		hdr = fld
		return nil
	}

	err := protoscan.ScanMessage(buf, protoscan.ObjectHeaderWithSignatureScheme, opts)
	if err != nil {
		return nil, err
	}

	if hdr == nil {
		return nil, errors.New("missing header")
	}
	if !withSig {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return nil, errors.New("missing signature")
	}

	return hdr, checkHeaderProtobufAgainstID(hdr, reqOID, hdrOrdered)
}
