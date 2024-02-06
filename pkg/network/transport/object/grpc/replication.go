package object

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	status "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
)

// ServeReplicateGRPC serves neo.fs.v2.object.ObjectService/Replicate RPC
// according to the gRPC protocol.
func (s *Server) ServeReplicateGRPC(_ context.Context, ln int, r io.Reader) (*objectGRPC.ReplicateResponse, error) {
	// TODO(@cthulhu-rider): limit ln?
	bufSize := ln
	if ln > gRPCMaxDataFrameSize {
		bufSize = gRPCMaxDataFrameSize
	}

	// context is ignored for 2 reasons:
	// 1. r is bound to it and will fail on abort
	// 2. if object replica is received, we continue processing regardless of
	// subsequent context for data safety: this could be critical if client was the
	// only replica holder but encountered an accident broken the connection
	code, msg := s.replicate(bufio.NewReaderSize(r, bufSize))
	// TODO: bufio.NewReaderSize does internal allocation visible during storm
	//  of tiny objects. Consider pool for such sizes and bytes.Reader

	// drain unknown fields asap to not clog the transport channel with transit data
	//
	// TODO(@cthulhu-rider): any better way to discard remaining unknown fields w/o
	// network transmission?
	//
	// TODO(@cthulhu-rider): do this inside gRPC lib
	_, errDiscard := io.Copy(io.Discard, r)
	if errDiscard != nil {
		s.log.Debug("failed to drain Replicate gRPC data frame stream", zap.Error(errDiscard))
	}

	return &objectGRPC.ReplicateResponse{Status: &status.Status{
		Code:    code,
		Message: msg,
	}}, nil
}

// replicate reads neo.fs.v2.object.ObjectService/Replicate request from r and
// serves it.
//
// TODO(@cthulhu-rider): pool all buffers
// TODO(@cthulhu-rider): support field merging
func (s *Server) replicate(r io.Reader) (uint32, string) {
	const (
		fieldNumObject    = 1
		fieldNumSignature = 2
	)

	var decodedSig bool
	var sig decodedSignature
	var decodedObj bool
	var obj decodedObject
	br := bufio.NewReaderSize(r, binary.MaxVarintLen64)

	defer func() {
		obj.releaseBuffers()
		sig.releaseBuffers()
	}()

	for {
		num, typ, err := readFieldTag(br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return codeInternal, fmt.Sprintf("invalid field tag: %v", err)
		}
		switch num {
		default:
			err = discardField(br, typ)
			if err != nil {
				return codeInternal, fmt.Sprintf("invalid field #%d (unknown): %v", num, err)
			}
		case fieldNumObject: // TODO: fast check if object signature decoded
			if decodedObj {
				return codeInternal, fmt.Sprintf("repeated field #%d (object)", num)
			}
			if typ != protowire.BytesType {
				return codeInternal, fmt.Sprintf("field #%d (object) is not of bytes type", num)
			}
			obj, err = decodeObject(br)
			if err != nil {
				return codeInternal, fmt.Sprintf("invalid field #%d (object): %v", num, err)
			}
			decodedObj = true
		case fieldNumSignature: // TODO: fast check if object already decoded
			if decodedSig {
				return codeInternal, fmt.Sprintf("repeated field #%d (object signature)", num)
			}
			if typ != protowire.BytesType {
				return codeInternal, fmt.Sprintf("field #%d (object signature) is not of bytes type", num)
			}
			sig, err = readSignatureField(br)
			if err != nil {
				return codeInternal, fmt.Sprintf("invalid field #%d (object signature): %v", num, err)
			}
			decodedSig = true
		}
		if decodedObj && decodedSig {
			break // ignore all possible unknown fields
		}
	}

	if !decodedObj {
		return codeInternal, fmt.Sprintf("%v #%d (object)", errMissingField, fieldNumObject)
	} else if !decodedSig {
		return codeInternal, fmt.Sprintf("%v #%d (object signature)", errMissingField, fieldNumSignature)
	}

	// TODO(@cthulhu-rider): discard unknown fields asap?

	if !sig.pubKey.Verify(obj.id, sig.value) {
		return codeInternal, "object signature mismatch"
	}

	cnr, ok := obj.hdr.ContainerID()
	if !ok {
		return codeInternal, "invalid object: missing container ID field"
	}
	ok, err := s.node.CompliesContainerStoragePolicy(cnr)
	if err != nil {
		if errors.Is(err, apistatus.ErrContainerNotFound) {
			return codeContainerNotFound, "failed to check server's compliance to object's storage policy: object's container not found"
		}
		return codeInternal, fmt.Sprintf("failed to check server's compliance to object's storage policy: %v", err)
	} else if !ok {
		return codeInternal, "server does not match the object's storage policy"
	}

	ok, err = s.node.ClientCompliesContainerStoragePolicy(sig.pubKeyBuf, cnr)
	if err != nil {
		if errors.Is(err, apistatus.ErrContainerNotFound) {
			return codeContainerNotFound, "failed to check client's compliance to object's storage policy: object's container not found"
		}
		return codeInternal, fmt.Sprintf("failed to check client's compliance to object's storage policy: %v", err)
	} else if !ok {
		return codeAccessDenied, "client does not match the object's storage policy"
	}

	err = s.node.StoreObject(cnr, obj.hdr, obj.hdrFld, obj.pldFld, obj.pldDataOff)
	if err != nil {
		return codeInternal, fmt.Sprintf("failed to store object locally: %v", err)
	}

	return 0, ""
}
