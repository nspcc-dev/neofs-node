package object

import (
	"context"
	"errors"
	"fmt"
	"io"

	igrpc "github.com/nspcc-dev/neofs-node/internal/grpc"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

// getStatusCodeFromResponseMetaHeader checks whether buf is a valid response
// meta header. If so, status code field is returned. In case of nesting
// headers, code from the root is returned.
//
// Absence of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed: if status field is repeated (including nested),
// code from the last one is returned.
func getStatusCodeFromResponseMetaHeader(buffers iprotobuf.BuffersSlice) (uint32, error) {
	var code uint32
	var gotOrigin bool

	var opts protoscan.ScanMessageOptions
	opts.InterceptNested = func(num protowire.Number, nested iprotobuf.BuffersSlice) error {
		switch num {
		default:
			return protoscan.ErrContinue
		case protosession.FieldResponseMetaHeaderOrigin:
			var err error
			code, err = getStatusCodeFromResponseMetaHeader(nested)
			if err != nil {
				return fmt.Errorf("handle origin field: %w", err)
			}
			gotOrigin = true
			return nil
		case protosession.FieldResponseMetaHeaderStatus:
			if gotOrigin {
				return protoscan.ErrContinue
			}
			var opts protoscan.ScanMessageOptions
			opts.InterceptUint32 = func(num protowire.Number, u uint32) error {
				if num == protostatus.FieldStatusCode {
					code = u
				}
				return nil
			}
			err := protoscan.ScanMessage(nested, protoscan.ResponseStatusScheme, opts)
			if err != nil {
				return fmt.Errorf("handle status field: %w", err)
			}
			return nil
		}
	}

	err := protoscan.ScanMessage(buffers, protoscan.ResponseMetaHeaderScheme, opts)
	if err != nil {
		return 0, err
	}

	return code, nil
}

type callServerStreamGRPCFunc = func(context.Context, *grpc.ClientConn, any) (grpc.ClientStream, error)

func forwardServerStreamRequest(ctx context.Context, req any, respStream grpc.ServerStream, node clientcore.MultiAddressClient, callFn callServerStreamGRPCFunc) error {
	err := node.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		return forwardServerStreamRequestGRPC(ctx, req, respStream, conn, callFn)
	})
	if errors.Is(err, clientcore.ErrAllConnectionsSkipped) {
		return getsvc.ErrUnavailableNode
	}

	return err
}

func forwardServerStreamRequestGRPC(ctx context.Context, req any, respStream grpc.ServerStream, conn *grpc.ClientConn, callFn callServerStreamGRPCFunc) error {
	stream, err := callFn(ctx, conn, req)
	if err != nil {
		if igrpc.IsUnavailable(err) {
			return clientcore.ErrSkipConnection
		}
		return err
	}

	for first := true; ; first = false {
		var respBuf mem.BufferSlice
		if err = stream.RecvMsg(&respBuf); err != nil {
			if first && igrpc.IsUnavailable(err) {
				return clientcore.ErrSkipConnection
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read response from stream: %w", err)
		}

		if err = respStream.SendMsg(respBuf); err != nil {
			return fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
		}
	}
}

func callUnary(ctx context.Context, conn *grpc.ClientConn, method string, req any) (mem.BufferSlice, error) {
	var respBuf mem.BufferSlice

	err := conn.Invoke(ctx, method, req, &respBuf,
		grpc.StaticMethod(),
		grpc.ForceCodecV2(iprotobuf.BufferedCodec{}),
	)
	if err != nil {
		return nil, fmt.Errorf("sending the request failed: %w", err)
	}

	return respBuf, nil
}
