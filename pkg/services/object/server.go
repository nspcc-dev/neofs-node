package object

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	protoobject "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	refsv2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	protosession "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/stat"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// GetObjectStream is an interface of NeoFS API v2 compatible object streamer.
type GetObjectStream interface {
	util.ServerStream
	Send(*v2object.GetResponse) error
}

// GetObjectRangeStream is an interface of NeoFS API v2 compatible payload range streamer.
type GetObjectRangeStream interface {
	util.ServerStream
	Send(*v2object.GetRangeResponse) error
}

// SearchStream is an interface of NeoFS API v2 compatible search streamer.
type SearchStream interface {
	util.ServerStream
	Send(*v2object.SearchResponse) error
}

// PutObjectStream is an interface of NeoFS API v2 compatible client's object streamer.
type PutObjectStream interface {
	Send(*v2object.PutRequest) error
	CloseAndRecv() (*v2object.PutResponse, error)
}

// ServiceServer is an interface of utility
// serving v2 Object service.
type ServiceServer interface {
	Get(*v2object.GetRequest, GetObjectStream) error
	Put(context.Context) (PutObjectStream, error)
	Head(context.Context, *v2object.HeadRequest) (*v2object.HeadResponse, error)
	Search(*v2object.SearchRequest, SearchStream) error
	Delete(context.Context, *v2object.DeleteRequest) (*v2object.DeleteResponse, error)
	GetRange(*v2object.GetRangeRequest, GetObjectRangeStream) error
	GetRangeHash(context.Context, *v2object.GetRangeHashRequest) (*v2object.GetRangeHashResponse, error)
}

// Various NeoFS protocol status codes.
const (
	codeInternal          = uint32(1024*protostatus.Section_SECTION_FAILURE_COMMON) + uint32(protostatus.CommonFail_INTERNAL)
	codeAccessDenied      = uint32(1024*protostatus.Section_SECTION_OBJECT) + uint32(protostatus.Object_ACCESS_DENIED)
	codeContainerNotFound = uint32(1024*protostatus.Section_SECTION_CONTAINER) + uint32(protostatus.Container_CONTAINER_NOT_FOUND)
)

// MetricCollector tracks exec statistics for the following ops:
//   - [stat.MethodObjectPut]
//   - [stat.MethodObjectGet]
//   - [stat.MethodObjectHead]
//   - [stat.MethodObjectDelete]
//   - [stat.MethodObjectSearch]
//   - [stat.MethodObjectRange]
//   - [stat.MethodObjectHash]
type MetricCollector interface {
	// HandleOpExecResult handles measured execution results of the given op.
	HandleOpExecResult(_ stat.Method, success bool, _ time.Duration)

	AddPutPayload(int)
	AddGetPayload(int)
}

// FSChain provides access to the FS chain required to serve NeoFS API Object
// service.
type FSChain interface {
	netmap.StateDetailed

	// ForEachContainerNodePublicKeyInLastTwoEpochs iterates over all nodes matching
	// the referenced container's storage policy at the current and the previous
	// NeoFS epochs, and passes their public keys into f. IterateContainerNodeKeys
	// breaks without an error when f returns false. Keys may be repeated.
	//
	// Returns [apistatus.ErrContainerNotFound] if referenced container was not
	// found.
	ForEachContainerNodePublicKeyInLastTwoEpochs(cid.ID, func(pubKey []byte) bool) error

	// IsOwnPublicKey checks whether given pubKey assigned to Node in the NeoFS
	// network map.
	IsOwnPublicKey(pubKey []byte) bool
}

// Storage groups ops of the node's object storage required to serve NeoFS API
// Object service.
type Storage interface {
	// VerifyAndStoreObject checks whether given object has correct format and, if
	// so, saves it into the Storage. StoreObject is called only when local node
	// complies with the container's storage policy.
	VerifyAndStoreObject(object.Object) error
}

type server struct {
	srv ServiceServer

	fsChain FSChain
	storage Storage
	signer  ecdsa.PrivateKey
	mNumber uint32
	metrics MetricCollector
}

// New provides protoobject.ObjectServiceServer for the given parameters.
func New(c ServiceServer, magicNumber uint32, fsChain FSChain, st Storage, signer ecdsa.PrivateKey, m MetricCollector) protoobject.ObjectServiceServer {
	return &server{
		srv:     c,
		fsChain: fsChain,
		storage: st,
		signer:  signer,
		mNumber: magicNumber,
		metrics: m,
	}
}

func (s *server) pushOpExecResult(op stat.Method, err error, startedAt time.Time) {
	s.metrics.HandleOpExecResult(op, err == nil, time.Since(startedAt))
}

func (s *server) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	v := version.Current()
	var v2 refsv2.Version
	v.WriteToV2(&v2)
	return &protosession.ResponseMetaHeader{
		Version: v2.ToGRPCMessage().(*refs.Version),
		Epoch:   s.fsChain.CurrentEpoch(),
		Status:  st,
	}
}

func (s *server) sendPutResponse(stream protoobject.ObjectService_PutServer, resp *protoobject.PutResponse) error {
	resp = util.SignResponse(&s.signer, resp, v2object.PutResponse{})
	return stream.SendAndClose(resp)
}

func (s *server) sendStatusPutResponse(stream protoobject.ObjectService_PutServer, err error) error {
	return s.sendPutResponse(stream, &protoobject.PutResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

// Put opens internal Object service Put stream and overtakes data from gRPC stream to it.
func (s *server) Put(gStream protoobject.ObjectService_PutServer) error {
	stream, err := s.srv.Put(gStream.Context())
	if err != nil {
		return err
	}

	t := time.Now()
	defer func() { s.pushOpExecResult(stat.MethodObjectPut, err, t) }()

	var req *protoobject.PutRequest
	var resp *v2object.PutResponse
	for {
		if req, err = gStream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				if resp, err = stream.CloseAndRecv(); err == nil {
					err = s.sendPutResponse(gStream, resp.ToGRPCMessage().(*protoobject.PutResponse)) // assign for defer
				} else {
					err = s.sendStatusPutResponse(gStream, err)
				}
			}
			return err
		}

		if c := req.GetBody().GetChunk(); c != nil {
			s.metrics.AddPutPayload(len(c))
		}

		putReq := new(v2object.PutRequest)
		if err = putReq.FromGRPCMessage(req); err != nil {
			return err
		}
		if err = signature.VerifyServiceMessage(putReq); err != nil {
			err = s.sendStatusPutResponse(gStream, util.ToRequestSignatureVerificationError(err)) // assign for defer
			return err
		}

		if err = stream.Send(putReq); err != nil {
			err = s.sendStatusPutResponse(gStream, err) // assign for defer
			return err
		}
	}
}

func (s *server) signDeleteResponse(resp *protoobject.DeleteResponse) *protoobject.DeleteResponse {
	return util.SignResponse(&s.signer, resp, v2object.DeleteResponse{})
}

func (s *server) makeStatusDeleteResponse(err error) *protoobject.DeleteResponse {
	return s.signDeleteResponse(&protoobject.DeleteResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

// Delete converts gRPC DeleteRequest message and passes it to internal Object service.
func (s *server) Delete(ctx context.Context, req *protoobject.DeleteRequest) (*protoobject.DeleteResponse, error) {
	delReq := new(v2object.DeleteRequest)
	err := delReq.FromGRPCMessage(req)
	if err != nil {
		return nil, err
	}

	t := time.Now()
	defer func() { s.pushOpExecResult(stat.MethodObjectDelete, err, t) }()

	if err = signature.VerifyServiceMessage(delReq); err != nil {
		return s.makeStatusDeleteResponse(err), nil
	}
	resp, err := s.srv.Delete(ctx, delReq)
	if err != nil {
		return s.makeStatusDeleteResponse(err), nil
	}

	return s.signDeleteResponse(resp.ToGRPCMessage().(*protoobject.DeleteResponse)), nil
}

func (s *server) signHeadResponse(resp *protoobject.HeadResponse) *protoobject.HeadResponse {
	return util.SignResponse(&s.signer, resp, v2object.HeadResponse{})
}

func (s *server) makeStatusHeadResponse(err error) *protoobject.HeadResponse {
	return s.signHeadResponse(&protoobject.HeadResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

// Head converts gRPC HeadRequest message and passes it to internal Object service.
func (s *server) Head(ctx context.Context, req *protoobject.HeadRequest) (*protoobject.HeadResponse, error) {
	searchReq := new(v2object.HeadRequest)
	err := searchReq.FromGRPCMessage(req)
	if err != nil {
		return nil, err
	}

	t := time.Now()
	defer func() { s.pushOpExecResult(stat.MethodObjectHead, err, t) }()

	if err = signature.VerifyServiceMessage(searchReq); err != nil {
		return s.makeStatusHeadResponse(err), nil
	}
	resp, err := s.srv.Head(ctx, searchReq)
	if err != nil {
		return s.makeStatusHeadResponse(err), nil
	}

	return s.signHeadResponse(resp.ToGRPCMessage().(*protoobject.HeadResponse)), nil
}

func (s *server) signHashResponse(resp *protoobject.GetRangeHashResponse) *protoobject.GetRangeHashResponse {
	return util.SignResponse(&s.signer, resp, v2object.GetRangeHashResponse{})
}

func (s *server) makeStatusHashResponse(err error) *protoobject.GetRangeHashResponse {
	return s.signHashResponse(&protoobject.GetRangeHashResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

// GetRangeHash converts gRPC GetRangeHashRequest message and passes it to internal Object service.
func (s *server) GetRangeHash(ctx context.Context, req *protoobject.GetRangeHashRequest) (*protoobject.GetRangeHashResponse, error) {
	hashRngReq := new(v2object.GetRangeHashRequest)
	err := hashRngReq.FromGRPCMessage(req)
	if err != nil {
		return nil, err
	}

	t := time.Now()
	defer func() { s.pushOpExecResult(stat.MethodObjectHash, err, t) }()
	if err = signature.VerifyServiceMessage(hashRngReq); err != nil {
		return s.makeStatusHashResponse(err), nil
	}
	resp, err := s.srv.GetRangeHash(ctx, hashRngReq)
	if err != nil {
		return s.makeStatusHashResponse(err), nil
	}

	return s.signHashResponse(resp.ToGRPCMessage().(*protoobject.GetRangeHashResponse)), nil
}

func (s *server) sendGetResponse(stream protoobject.ObjectService_GetServer, resp *protoobject.GetResponse) error {
	return stream.Send(util.SignResponse(&s.signer, resp, v2object.GetResponse{}))
}

func (s *server) sendStatusGetResponse(stream protoobject.ObjectService_GetServer, err error) error {
	return s.sendGetResponse(stream, &protoobject.GetResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

type getStreamerV2 struct {
	protoobject.ObjectService_GetServer
	srv *server
}

func (s *getStreamerV2) Send(resp *v2object.GetResponse) error {
	r := resp.ToGRPCMessage().(*protoobject.GetResponse)
	if c := r.GetBody().GetChunk(); c != nil {
		s.srv.metrics.AddGetPayload(len(c))
	}
	return s.srv.sendGetResponse(s.ObjectService_GetServer, r)
}

// Get converts gRPC GetRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *server) Get(req *protoobject.GetRequest, gStream protoobject.ObjectService_GetServer) error {
	getReq := new(v2object.GetRequest)
	err := getReq.FromGRPCMessage(req)
	if err != nil {
		return err
	}
	t := time.Now()
	defer func() { s.pushOpExecResult(stat.MethodObjectGet, err, t) }()
	if err = signature.VerifyServiceMessage(getReq); err != nil {
		return s.sendStatusGetResponse(gStream, err)
	}
	err = s.srv.Get(
		getReq,
		&getStreamerV2{
			ObjectService_GetServer: gStream,
			srv:                     s,
		},
	)
	if err != nil {
		return s.sendStatusGetResponse(gStream, err)
	}
	return nil
}

func (s *server) sendRangeResponse(stream protoobject.ObjectService_GetRangeServer, resp *protoobject.GetRangeResponse) error {
	return stream.Send(util.SignResponse(&s.signer, resp, v2object.GetRangeResponse{}))
}

func (s *server) sendStatusRangeResponse(stream protoobject.ObjectService_GetRangeServer, err error) error {
	return s.sendRangeResponse(stream, &protoobject.GetRangeResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

type getRangeStreamerV2 struct {
	protoobject.ObjectService_GetRangeServer
	srv *server
}

func (s *getRangeStreamerV2) Send(resp *v2object.GetRangeResponse) error {
	return s.srv.sendRangeResponse(s.ObjectService_GetRangeServer, resp.ToGRPCMessage().(*protoobject.GetRangeResponse))
}

// GetRange converts gRPC GetRangeRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *server) GetRange(req *protoobject.GetRangeRequest, gStream protoobject.ObjectService_GetRangeServer) error {
	getRngReq := new(v2object.GetRangeRequest)
	err := getRngReq.FromGRPCMessage(req)
	if err != nil {
		return err
	}
	t := time.Now()
	defer func() { s.pushOpExecResult(stat.MethodObjectRange, err, t) }()
	if err = signature.VerifyServiceMessage(getRngReq); err != nil {
		return s.sendStatusRangeResponse(gStream, err)
	}
	err = s.srv.GetRange(
		getRngReq,
		&getRangeStreamerV2{
			ObjectService_GetRangeServer: gStream,
			srv:                          s,
		},
	)
	if err != nil {
		return s.sendStatusRangeResponse(gStream, err)
	}
	return nil
}

func (s *server) sendSearchResponse(stream protoobject.ObjectService_SearchServer, resp *protoobject.SearchResponse) error {
	return stream.Send(util.SignResponse(&s.signer, resp, v2object.SearchResponse{}))
}

func (s *server) sendStatusSearchResponse(stream protoobject.ObjectService_SearchServer, err error) error {
	return s.sendSearchResponse(stream, &protoobject.SearchResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

type searchStreamerV2 struct {
	protoobject.ObjectService_SearchServer
	srv *server
}

func (s *searchStreamerV2) Send(resp *v2object.SearchResponse) error {
	return s.srv.sendSearchResponse(s.ObjectService_SearchServer, resp.ToGRPCMessage().(*protoobject.SearchResponse))
}

// Search converts gRPC SearchRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *server) Search(req *protoobject.SearchRequest, gStream protoobject.ObjectService_SearchServer) error {
	searchReq := new(v2object.SearchRequest)
	err := searchReq.FromGRPCMessage(req)
	if err != nil {
		return err
	}
	t := time.Now()
	defer func() { s.pushOpExecResult(stat.MethodObjectSearch, err, t) }()
	if err = signature.VerifyServiceMessage(searchReq); err != nil {
		return s.sendStatusSearchResponse(gStream, err)
	}
	err = s.srv.Search(
		searchReq,
		&searchStreamerV2{
			ObjectService_SearchServer: gStream,
			srv:                        s,
		},
	)
	if err != nil {
		return s.sendStatusSearchResponse(gStream, err)
	}
	return nil
}

// Replicate serves neo.fs.v2.object.ObjectService/Replicate RPC.
func (s *server) Replicate(_ context.Context, req *protoobject.ReplicateRequest) (*protoobject.ReplicateResponse, error) {
	if req.Object == nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeInternal, Message: "binary object field is missing/empty",
		}}, nil
	}

	if req.Object.ObjectId == nil || len(req.Object.ObjectId.Value) == 0 {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeInternal, Message: "ID field is missing/empty in the object field",
		}}, nil
	}

	if req.Signature == nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeInternal, Message: "missing object signature field",
		}}, nil
	}

	if len(req.Signature.Key) == 0 {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeInternal, Message: "public key field is missing/empty in the object signature field",
		}}, nil
	}

	if len(req.Signature.Sign) == 0 {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeInternal, Message: "signature value is missing/empty in the object signature field",
		}}, nil
	}

	switch scheme := req.Signature.Scheme; scheme {
	default:
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
			Message: "unsupported scheme in the object signature field",
		}}, nil
	case
		refs.SignatureScheme_ECDSA_SHA512,
		refs.SignatureScheme_ECDSA_RFC6979_SHA256,
		refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT:
	}

	hdr := req.Object.GetHeader()
	if hdr == nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
			Message: "missing header field in the object field",
		}}, nil
	}

	gCnrMsg := hdr.GetContainerId()
	if gCnrMsg == nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
			Message: "missing container ID field in the object header field",
		}}, nil
	}

	var cnr cid.ID
	var cnrMsg refsv2.ContainerID
	err := cnrMsg.FromGRPCMessage(gCnrMsg)
	if err == nil {
		err = cnr.ReadFromV2(cnrMsg)
	}
	if err != nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
			Message: fmt.Sprintf("invalid container ID in the object header field: %v", err),
		}}, nil
	}

	var pubKey neofscrypto.PublicKey
	switch req.Signature.Scheme {
	// other cases already checked above
	case refs.SignatureScheme_ECDSA_SHA512:
		pubKey = new(neofsecdsa.PublicKey)
		err = pubKey.Decode(req.Signature.Key)
		if err != nil {
			return &protoobject.ReplicateResponse{Status: &protostatus.Status{
				Code:    codeInternal,
				Message: "invalid ECDSA public key in the object signature field",
			}}, nil
		}
	case refs.SignatureScheme_ECDSA_RFC6979_SHA256:
		pubKey = new(neofsecdsa.PublicKeyRFC6979)
		err = pubKey.Decode(req.Signature.Key)
		if err != nil {
			return &protoobject.ReplicateResponse{Status: &protostatus.Status{
				Code:    codeInternal,
				Message: "invalid ECDSA public key in the object signature field",
			}}, nil
		}
	case refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT:
		pubKey = new(neofsecdsa.PublicKeyWalletConnect)
		err = pubKey.Decode(req.Signature.Key)
		if err != nil {
			return &protoobject.ReplicateResponse{Status: &protostatus.Status{
				Code:    codeInternal,
				Message: "invalid ECDSA public key in the object signature field",
			}}, nil
		}
	}
	if !pubKey.Verify(req.Object.ObjectId.Value, req.Signature.Sign) {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
			Message: "signature mismatch in the object signature field",
		}}, nil
	}

	var clientInCnr, serverInCnr bool
	err = s.fsChain.ForEachContainerNodePublicKeyInLastTwoEpochs(cnr, func(pubKey []byte) bool {
		if !serverInCnr {
			serverInCnr = s.fsChain.IsOwnPublicKey(pubKey)
		}
		if !clientInCnr {
			clientInCnr = bytes.Equal(pubKey, req.Signature.Key)
		}
		return !clientInCnr || !serverInCnr
	})
	if err != nil {
		if errors.Is(err, apistatus.ErrContainerNotFound) {
			return &protoobject.ReplicateResponse{Status: &protostatus.Status{
				Code:    codeContainerNotFound,
				Message: "failed to check server's compliance to object's storage policy: object's container not found",
			}}, nil
		}

		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
			Message: fmt.Sprintf("failed to apply object's storage policy: %v", err),
		}}, nil
	} else if !serverInCnr {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeAccessDenied, Message: "server does not match the object's storage policy",
		}}, nil
	} else if !clientInCnr {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeAccessDenied, Message: "client does not match the object's storage policy",
		}}, nil
	}

	// TODO(@cthulhu-rider): avoid decoding the object completely
	obj, err := objectFromMessage(req.Object)
	if err != nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
			Message: fmt.Sprintf("invalid object field: %v", err),
		}}, nil
	}

	err = s.storage.VerifyAndStoreObject(*obj)
	if err != nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
			Message: fmt.Sprintf("failed to verify and store object locally: %v", err),
		}}, nil
	}

	resp := new(protoobject.ReplicateResponse)
	if req.GetSignObject() {
		resp.ObjectSignature, err = s.metaInfoSignature(*obj)
		if err != nil {
			return &protoobject.ReplicateResponse{Status: &protostatus.Status{
				Code:    codeInternal,
				Message: fmt.Sprintf("failed to sign object meta information: %v", err),
			}}, nil
		}
	}

	return resp, nil
}

func objectFromMessage(gMsg *protoobject.Object) (*object.Object, error) {
	var msg v2object.Object
	err := msg.FromGRPCMessage(gMsg)
	if err != nil {
		return nil, err
	}

	return object.NewFromV2(&msg), nil
}

func (s *server) metaInfoSignature(o object.Object) ([]byte, error) {
	firstObj := o.GetFirstID()
	if o.HasParent() && firstObj.IsZero() {
		// object itself is the first one
		firstObj = o.GetID()
	}
	prevObj := o.GetPreviousID()

	var deleted []oid.ID
	var locked []oid.ID
	typ := o.Type()
	switch typ {
	case object.TypeTombstone:
		var t object.Tombstone
		err := t.Unmarshal(o.Payload())
		if err != nil {
			return nil, fmt.Errorf("reading tombstoned objects: %w", err)
		}

		deleted = t.Members()
	case object.TypeLock:
		var l object.Lock
		err := l.Unmarshal(o.Payload())
		if err != nil {
			return nil, fmt.Errorf("reading locked objects: %w", err)
		}

		locked = make([]oid.ID, l.NumberOfMembers())
		l.ReadMembers(locked)
	default:
	}

	currentBlock := s.fsChain.CurrentBlock()
	currentEpochDuration := s.fsChain.CurrentEpochDuration()
	firstBlock := (uint64(currentBlock)/currentEpochDuration + 1) * currentEpochDuration
	secondBlock := firstBlock + currentEpochDuration
	thirdBlock := secondBlock + currentEpochDuration

	firstMeta := objectcore.EncodeReplicationMetaInfo(o.GetContainerID(), o.GetID(), firstObj, prevObj, o.PayloadSize(), typ, deleted, locked, firstBlock, s.mNumber)
	secondMeta := objectcore.EncodeReplicationMetaInfo(o.GetContainerID(), o.GetID(), firstObj, prevObj, o.PayloadSize(), typ, deleted, locked, secondBlock, s.mNumber)
	thirdMeta := objectcore.EncodeReplicationMetaInfo(o.GetContainerID(), o.GetID(), firstObj, prevObj, o.PayloadSize(), typ, deleted, locked, thirdBlock, s.mNumber)

	var firstSig neofscrypto.Signature
	var secondSig neofscrypto.Signature
	var thirdSig neofscrypto.Signature
	signer := neofsecdsa.SignerRFC6979(s.signer)
	err := firstSig.Calculate(signer, firstMeta)
	if err != nil {
		return nil, fmt.Errorf("signature failure: %w", err)
	}
	err = secondSig.Calculate(signer, secondMeta)
	if err != nil {
		return nil, fmt.Errorf("signature failure: %w", err)
	}
	err = thirdSig.Calculate(signer, thirdMeta)
	if err != nil {
		return nil, fmt.Errorf("signature failure: %w", err)
	}

	firstSigV2 := new(refsv2.Signature)
	firstSig.WriteToV2(firstSigV2)
	secondSigV2 := new(refsv2.Signature)
	secondSig.WriteToV2(secondSigV2)
	thirdSigV2 := new(refsv2.Signature)
	thirdSig.WriteToV2(thirdSigV2)

	res := make([]byte, 0, 4+firstSigV2.StableSize()+4+secondSigV2.StableSize()+4+thirdSigV2.StableSize())
	res = binary.LittleEndian.AppendUint32(res, uint32(firstSigV2.StableSize()))
	res = append(res, firstSigV2.StableMarshal(nil)...)
	res = binary.LittleEndian.AppendUint32(res, uint32(secondSigV2.StableSize()))
	res = append(res, secondSigV2.StableMarshal(nil)...)
	res = binary.LittleEndian.AppendUint32(res, uint32(thirdSigV2.StableSize()))
	res = append(res, thirdSigV2.StableMarshal(nil)...)

	return res, nil
}
