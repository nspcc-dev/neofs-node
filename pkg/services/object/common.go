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
	"github.com/nspcc-dev/neofs-node/pkg/services/object/common"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	missingRequestBodyMessage       = "missing request body"
	invalidRequestBodyMessage       = "invalid request body"
	invalidRequestMetaHeaderMessage = "invalid request meta header"
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

func fetchRequiredObjectAddress(m *protorefs.Address) (cid.ID, oid.ID, error) {
	if m == nil {
		return cid.ID{}, oid.ID{}, errors.New("missing object address")
	}

	cnr, err := fetchRequiredContainerID(m.ContainerId)
	if err != nil {
		return cid.ID{}, oid.ID{}, fmt.Errorf("invalid object address: %w", err)
	}

	obj, err := fetchRequiredObjectID(m.ObjectId)
	if err != nil {
		return cid.ID{}, oid.ID{}, fmt.Errorf("invalid object address: %w", err)
	}

	return cnr, obj, nil
}

func fetchRequiredContainerID(m *protorefs.ContainerID) (cid.ID, error) {
	if m == nil {
		return cid.ID{}, errors.New("missing container ID")
	}

	var res cid.ID
	if err := res.FromProtoMessage(m); err != nil {
		return cid.ID{}, fmt.Errorf("invalid container ID: %w", err)
	}

	return res, nil
}

func fetchRequiredObjectID(m *protorefs.ObjectID) (oid.ID, error) {
	if m == nil {
		return oid.ID{}, errors.New("missing object ID")
	}
	return decodeObjectID(m)
}

func fetchOptionalObjectID(m *protorefs.ObjectID) (oid.ID, error) {
	if m == nil {
		return oid.ID{}, nil
	}
	return decodeObjectID(m)
}

func decodeObjectID(m *protorefs.ObjectID) (oid.ID, error) {
	var res oid.ID
	if err := res.FromProtoMessage(m); err != nil {
		return oid.ID{}, fmt.Errorf("invalid object ID: %w", err)
	}
	return res, nil
}

type requestMetadata struct {
	tokens                common.RequestTokens
	sessionTokenMessage   *protosession.SessionTokenV2
	sessionV1TokenMessage *protosession.SessionToken
	ttl                   uint32
	xHeaders              []*protosession.XHeader
}

func (s *Server) handleRequestMetaHeader(metaHdr *protosession.RequestMetaHeader, reqVerb sessionv2.Verb, reqVerbV1 session.ObjectVerb, reqCnr cid.ID, reqObj oid.ID) (requestMetadata, error) {
	reqMD, err := s._handleRequestMetaHeader(metaHdr, reqVerb, reqVerbV1, reqCnr, reqObj)
	if err != nil {
		if errors.Is(err, apistatus.Error) {
			return requestMetadata{}, err
		}
		return requestMetadata{}, newBadRequestError(invalidRequestMetaHeaderMessage + ": " + err.Error())
	}

	return reqMD, nil
}

func (s *Server) _handleRequestMetaHeader(metaHdr *protosession.RequestMetaHeader, reqVerb sessionv2.Verb, reqVerbV1 session.ObjectVerb, reqCnr cid.ID, reqObj oid.ID) (requestMetadata, error) {
	if metaHdr == nil {
		return requestMetadata{}, nil
	}

	var reqMD requestMetadata
	reqMD.ttl = metaHdr.Ttl

	for origin := metaHdr.GetOrigin(); origin != nil; origin = metaHdr.GetOrigin() {
		metaHdr = origin
	}

	if metaHdr.SessionToken != nil && metaHdr.SessionTokenV2 != nil {
		return requestMetadata{}, errors.New("both V1 and V2 session tokens are set")
	}

	if metaHdr.SessionTokenV2 != nil {
		token, err := s.reqInfoProc.VerifySessionTokenMessage(metaHdr.SessionTokenV2, reqVerb, reqCnr)
		if err != nil {
			return requestMetadata{}, err
		}
		reqMD.tokens.Session = &token
	} else if metaHdr.SessionToken != nil {
		token, err := s.reqInfoProc.VerifySessionV1TokenMessage(metaHdr.SessionToken, reqVerbV1, reqCnr, reqObj)
		if err != nil {
			return requestMetadata{}, err
		}
		reqMD.tokens.SessionV1 = &token
	}

	if metaHdr.BearerToken != nil {
		token, err := s.reqInfoProc.VerifyBearerTokenMessage(metaHdr.BearerToken)
		if err != nil {
			return requestMetadata{}, err
		}
		reqMD.tokens.Bearer = &token
	}

	reqMD.xHeaders = metaHdr.XHeaders
	reqMD.sessionTokenMessage = metaHdr.SessionTokenV2
	reqMD.sessionV1TokenMessage = metaHdr.SessionToken

	return reqMD, nil
}
