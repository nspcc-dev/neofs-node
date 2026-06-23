package object

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	igrpc "github.com/nspcc-dev/neofs-node/internal/grpc"
	inetmap "github.com/nspcc-dev/neofs-node/internal/netmap"
	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	metasvc "github.com/nspcc-dev/neofs-node/pkg/services/meta"
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/common"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoacl "github.com/nspcc-dev/neofs-sdk-go/proto/acl"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/stat"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Handlers represents storage node's internal handler Object service op
// payloads.
type Handlers interface {
	Get(context.Context, getsvc.Prm) error
	Put(context.Context) (*putsvc.Streamer, error)
	Head(context.Context, getsvc.HeadPrm) error
	Delete(context.Context, deletesvc.Prm) error
	GetRange(context.Context, getsvc.RangePrm) error
}

// Various NeoFS protocol status codes.
const (
	codeInternal          = uint32(1024*protostatus.Section_SECTION_FAILURE_COMMON) + uint32(protostatus.CommonFail_INTERNAL)
	codeBadRequest        = uint32(1024*protostatus.Section_SECTION_FAILURE_COMMON) + uint32(protostatus.CommonFail_BAD_REQUEST)
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
type MetricCollector interface {
	// HandleOpExecResult handles measured execution results of the given op.
	HandleOpExecResult(_ stat.Method, success bool, _ time.Duration)

	AddPutPayload(int)
	AddGetPayload(int)
}

// FSChain provides access to the FS chain required to serve NeoFS API Object
// service.
type FSChain interface {
	containercore.Source
	netmapcore.StateDetailed
	icrypto.N3ScriptRunner

	// ForEachContainerNodePublicKeyInLastTwoEpochs iterates over all nodes matching
	// the referenced container's storage policy at the current and the previous
	// NeoFS epochs, and passes their public keys into f. IterateContainerNodeKeys
	// breaks without an error when f returns false. Keys may be repeated.
	//
	// Returns [apistatus.ErrContainerNotFound] if referenced container was not
	// found.
	ForEachContainerNodePublicKeyInLastTwoEpochs(cid.ID, func(pubKey []byte) bool) error

	// SelectContainerNodes applies referenced container's storage policy to the
	// current network map and returns matching nodes along with the applied rules.
	//
	// Returns [apistatus.ErrContainerNotFound] if referenced container was not
	// found.
	SelectContainerNodes(cnr cid.ID) ([][]netmap.NodeInfo, []uint, []iec.Rule, error)

	// IsOwnPublicKey checks whether given pubKey assigned to Node in the NeoFS
	// network map.
	IsOwnPublicKey(pubKey []byte) bool

	// LocalNodeUnderMaintenance checks whether local node is under maintenance
	// according to the network map from FSChain.
	LocalNodeUnderMaintenance() bool
}

type sessions interface {
	// GetSessionPrivateKey reads private session key by his account.
	// Returns [apistatus.ErrSessionTokenNotFound] if there is no data for the
	// referenced session.
	GetSessionPrivateKey(account user.ID) (ecdsa.PrivateKey, error)

	// GetSessionV2PrivateKey reads private session key by session
	// subject. Returns [apistatus.ErrSessionTokenNotFound] if there is no data
	// for the referenced session.
	GetSessionV2PrivateKey(subject []sessionv2.Target) (ecdsa.PrivateKey, error)
}

// Storage groups ops of the node's storage required to serve NeoFS API Object
// service.
type Storage interface {
	sessions

	// VerifyAndStoreObjectLocally checks whether given object has correct format
	// and, if so, saves it in the Storage. StoreObject is called only when local
	// node complies with the container's storage policy.
	VerifyAndStoreObjectLocally(context.Context, object.Object) error

	// SearchObjects selects up to count container's objects from the given
	// container matching the specified filters.
	SearchObjects(_ context.Context, _ cid.ID, _ []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error)
}

// ACLInfoExtractor is the interface that allows to fetch data required for ACL
// checks from various types of grpc requests.
type ACLInfoExtractor interface {
	PutRequestToInfo(*protoobject.PutRequest, *protoobject.PutRequest_Body_Init, cid.ID, acl.Op, common.RequestTokens) (aclsvc.RequestInfo, user.ID, error)
	DeleteRequestToInfo(*protoobject.DeleteRequest, cid.ID, common.RequestTokens) (aclsvc.RequestInfo, error)
	HeadRequestToInfo(*protoobject.HeadRequest, cid.ID, common.RequestTokens) (aclsvc.RequestInfo, error)
	GetRequestToInfo(*protoobject.GetRequest, cid.ID, common.RequestTokens) (aclsvc.RequestInfo, error)
	RangeRequestToInfo(*protoobject.GetRangeRequest, cid.ID, common.RequestTokens) (aclsvc.RequestInfo, error)
	SearchV2RequestToInfo(*protoobject.SearchV2Request, cid.ID, common.RequestTokens) (aclsvc.RequestInfo, error)
	VerifySessionTokenMessage(*protosession.SessionTokenV2, sessionv2.Verb, cid.ID) (sessionv2.Token, error)
	VerifySessionV1TokenMessage(*protosession.SessionToken, session.ObjectVerb, cid.ID, oid.ID) (session.Object, error)
	VerifyBearerTokenMessage(*protoacl.BearerToken) (bearer.Token, error)
}

// ClientConstructor returns a client for given node.
type ClientConstructor interface {
	Get(context.Context, netmap.NodeInfo) (clientcore.MultiAddressClient, error)
}

const accessDeniedACLReasonFmt = "access to operation %s is denied by basic ACL check"
const accessDeniedEACLReasonFmt = "access to operation %s is denied by extended ACL check: %v"

func basicACLErr(info aclsvc.RequestInfo) error {
	var errAccessDenied apistatus.ObjectAccessDenied
	errAccessDenied.WriteReason(fmt.Sprintf(accessDeniedACLReasonFmt, info.Operation))

	return errAccessDenied
}

func eACLErr(info aclsvc.RequestInfo, err error) error {
	var errAccessDenied apistatus.ObjectAccessDenied
	errAccessDenied.WriteReason(fmt.Sprintf(accessDeniedEACLReasonFmt, info.Operation, err))

	return errAccessDenied
}

const (
	maxRespMsgSize       = 4 << 20                            // default gRPC limit
	maxRespDataChunkSize = maxRespMsgSize * 3 / 4             // 25% to meta, 75% to payload
	addrMsgSize          = 72                                 // 32 bytes object ID, 32 bytes container ID, 8 bytes protobuf encoding
	maxObjAddrRespAmount = maxRespDataChunkSize / addrMsgSize // each address is about 72 bytes
)

// Server represents Object Service server that provides object manipulation
// operations including Get, Put, Head, Range, Delete, Search, and Replicate.
// The server enforces access control, verifies requests, and handles data storage
// and retrieval operations.
type Server struct {
	handlers      Handlers
	fsChain       FSChain
	storage       Storage
	meta          *metasvc.Meta
	signer        ecdsa.PrivateKey
	pubKeyBytes   []byte
	metrics       MetricCollector
	aclChecker    aclsvc.ACLChecker
	reqInfoProc   ACLInfoExtractor
	nodeClients   ClientConstructor
	searchWorkers *ants.Pool
	log           *zap.Logger
}

// New provides protoobject.ObjectServiceServer for the given parameters.
func New(hs Handlers, sp *ants.Pool, fsChain FSChain, st Storage, metaSvc *metasvc.Meta, signer ecdsa.PrivateKey, m MetricCollector, ac aclsvc.ACLChecker, rp ACLInfoExtractor, cs ClientConstructor, log *zap.Logger) *Server {
	pubKeyBytes := (*keys.PublicKey)(&signer.PublicKey).Bytes()
	if len(pubKeyBytes) != compressedECDSAPublicKeyLen {
		panic(fmt.Sprintf("wrong public key len: expected %d, got %d", compressedECDSAPublicKeyLen, len(pubKeyBytes)))
	}

	return &Server{
		handlers:      hs,
		fsChain:       fsChain,
		storage:       st,
		meta:          metaSvc,
		signer:        signer,
		pubKeyBytes:   pubKeyBytes,
		metrics:       m,
		aclChecker:    ac,
		reqInfoProc:   rp,
		nodeClients:   cs,
		searchWorkers: sp,
		log:           log,
	}
}

func (s *Server) pushOpExecResult(op stat.Method, err error, startedAt time.Time) {
	s.metrics.HandleOpExecResult(op, err == nil, time.Since(startedAt))
}

func newCurrentProtoVersionMessage() *refs.Version {
	return version.Current().ProtoMessage()
}

func (s *Server) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: newCurrentProtoVersionMessage(),
		Epoch:   s.fsChain.CurrentEpoch(),
		Status:  st,
	}
}

func (s *Server) sendPutResponse(stream protoobject.ObjectService_PutServer, resp *protoobject.PutResponse, err error, req *protoobject.PutRequest) error {
	if resp == nil {
		resp = new(protoobject.PutResponse)
	}
	if err != nil {
		resp.MetaHeader = s.makeResponseMetaHeader(util.ToStatus(err))
	}

	resp.VerifyHeader = util.SignResponseIfNeeded(&s.signer, resp, req)
	return stream.SendAndClose(resp)
}

func (s *Server) sendStatusPutResponse(stream protoobject.ObjectService_PutServer, err error, req *protoobject.PutRequest) error {
	return s.sendPutResponse(stream, nil, err, req)
}

type putStream struct {
	ctx    context.Context
	signer ecdsa.PrivateKey
	base   *putsvc.Streamer

	cacheReqs bool
	initReq   *protoobject.PutRequest
	chunkReqs []*protoobject.PutRequest

	expBytes, recvBytes uint64 // payload
}

func newIntermediatePutStream(signer ecdsa.PrivateKey, base *putsvc.Streamer, ctx context.Context) *putStream {
	return &putStream{
		ctx:    ctx,
		signer: signer,
		base:   base,
	}
}

func (x *putStream) sendToRemoteNode(c clientcore.MultiAddressClient) error {
	return c.ForAnyGRPCConn(x.ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		return putToRemoteNode(ctx, conn, x.initReq, x.chunkReqs) // TODO: log error
	})
}

func putToRemoteNode(ctx context.Context, conn *grpc.ClientConn, initReq *protoobject.PutRequest, chunkReqs []*protoobject.PutRequest) error {
	stream, err := protoobject.NewObjectServiceClient(conn).Put(ctx)
	if err != nil {
		return fmt.Errorf("stream opening failed: %w", err)
	}

	err = stream.Send(initReq)
	if err != nil {
		return fmt.Errorf("sending the initial message to stream failed: %w", err)
	}
	for i := range chunkReqs {
		if err := stream.Send(chunkReqs[i]); err != nil {
			return fmt.Errorf("sending the chunk %d failed: %w", i, err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("closing the stream failed: %w", err)
	}

	if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
		return fmt.Errorf("remote node response: %w", err)
	}
	return nil
}

func (x *putStream) resignRequest(req *protoobject.PutRequest) (*protoobject.PutRequest, error) {
	meta := req.GetMetaHeader()
	if meta == nil {
		return nil, errors.New("missing meta header")
	}
	req.MetaHeader = &protosession.RequestMetaHeader{
		Ttl:    meta.GetTtl() - 1,
		Origin: meta,
	}
	var err error
	req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(neofsecdsa.Signer(x.signer), req, nil)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (x *putStream) forwardInitRequest(req *protoobject.PutRequest, initPart *protoobject.PutRequest_Body_Init, reqMD requestMetadata) error {
	mo := &protoobject.Object{
		ObjectId:  initPart.ObjectId,
		Signature: initPart.Signature,
		Header:    initPart.Header,
	}
	var obj = new(object.Object)
	err := obj.FromProtoMessage(mo)
	if err != nil {
		return err
	}

	var p putsvc.PutInitPrm
	p.WithCommonPrm(objutil.CommonPrmFromRequest(reqMD.ttl, reqMD.xHeaders, reqMD.tokens))
	p.WithObject(obj)
	p.WithRelay(x.sendToRemoteNode)
	if err = x.base.Init(&p); err != nil {
		return fmt.Errorf("could not init object put stream: %w", err)
	}

	if x.cacheReqs = initPart.Signature != nil; !x.cacheReqs {
		return nil
	}

	x.expBytes = initPart.Header.GetPayloadLength()
	if m := x.base.MaxObjectSize(); x.expBytes > m {
		return putsvc.ErrExceedingMaxSize
	}
	signed, err := x.resignRequest(req) // TODO: resign only when needed
	if err != nil {
		return err // TODO: add context
	}
	x.initReq = signed
	return nil
}

func (x *putStream) forwardChunkRequest(req *protoobject.PutRequest, c []byte) error {
	if x.cacheReqs {
		if x.recvBytes += uint64(len(c)); x.recvBytes > x.expBytes {
			return putsvc.ErrWrongPayloadSize
		}
	}
	if err := x.base.SendChunk(new(putsvc.PutChunkPrm).WithChunk(c)); err != nil {
		return fmt.Errorf("could not send payload chunk: %w", err)
	}
	if !x.cacheReqs {
		return nil
	}
	signed, err := x.resignRequest(req) // TODO: resign only when needed
	if err != nil {
		return err // TODO: add context
	}
	x.chunkReqs = append(x.chunkReqs, signed)
	return nil
}

func (x *putStream) close() (*protoobject.PutResponse, error) {
	if x.cacheReqs && x.recvBytes != x.expBytes {
		return nil, putsvc.ErrWrongPayloadSize
	}

	id, err := x.base.Close()
	if err != nil {
		err = fmt.Errorf("close put stream: %w", err)
		if !errors.Is(err, apistatus.ErrIncomplete) {
			return nil, err
		}
	}

	return &protoobject.PutResponse{
		Body: &protoobject.PutResponse_Body{
			ObjectId: id.ProtoMessage(),
		},
	}, err
}

func (s *Server) Put(gStream protoobject.ObjectService_PutServer) error {
	t := time.Now()
	stream, err := s.handlers.Put(gStream.Context())

	defer func() { s.pushOpExecResult(stat.MethodObjectPut, err, t) }()
	if err != nil {
		return err
	}

	var req, reqFirst *protoobject.PutRequest
	var resp *protoobject.PutResponse

	ps := newIntermediatePutStream(s.signer, stream, gStream.Context())
	for {
		if req, err = gStream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				resp, err = ps.close()
				err = s.sendPutResponse(gStream, resp, err, reqFirst)
				return err
			}
			return err
		}

		if reqFirst == nil {
			reqFirst = req
		}

		if c := req.GetBody().GetChunk(); c != nil {
			s.metrics.AddPutPayload(len(c))
		}

		if err = icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
			err = s.sendStatusPutResponse(gStream, err, reqFirst) // assign for defer
			return err
		}

		if s.fsChain.LocalNodeUnderMaintenance() {
			return s.sendStatusPutResponse(gStream, apistatus.ErrNodeUnderMaintenance, reqFirst)
		}

		if req.Body == nil {
			err = newBadRequestError(missingRequestBodyMessage) // defer
			return err
		}

		var initPart *protoobject.PutRequest_Body_Init

		switch v := req.GetBody().GetObjectPart().(type) {
		default:
			err = s.sendStatusPutResponse(gStream, fmt.Errorf("invalid object put stream part type %T", v), reqFirst) // assign for defer
			return err
		case *protoobject.PutRequest_Body_Init_:
			if v.Init == nil {
				err = newBadRequestError(invalidRequestBodyMessage + ": missing init field") // defer
				return s.sendStatusPutResponse(gStream, err, reqFirst)
			}
			initPart = v.Init
		case *protoobject.PutRequest_Body_Chunk:
			if err = ps.forwardChunkRequest(req, v.Chunk); err != nil {
				err = s.sendStatusPutResponse(gStream, err, reqFirst) // assign for defer
				return err
			}
			continue
		}

		hdr := initPart.Header
		if hdr == nil {
			err = newBadRequestError(invalidPutRequestInitFieldMessage + ": missing header field") // defer
			return s.sendStatusPutResponse(gStream, err, reqFirst)
		}

		var cnrID cid.ID
		cnrID, err = fetchRequiredContainerID(hdr.ContainerId)
		if err != nil {
			err = newBadRequestError(invalidPutRequestInitFieldMessage + ": invalid header field: " + err.Error()) // defer
			return s.sendStatusPutResponse(gStream, err, reqFirst)
		}

		var objID oid.ID
		objID, err = fetchOptionalObjectID(initPart.ObjectId)
		if err != nil {
			err = newBadRequestError(invalidPutRequestInitFieldMessage + ": invalid header field: " + err.Error()) // defer
			return s.sendStatusPutResponse(gStream, err, reqFirst)
		}

		op, verb, verbV1 := acl.OpObjectPut, sessionv2.VerbObjectPut, session.VerbObjectPut
		tombstone := initPart.GetHeader().GetObjectType() == protoobject.ObjectType_TOMBSTONE
		if tombstone {
			// such objects are specific - saving them is essentially the removal of other
			// objects
			op, verb, verbV1 = acl.OpObjectDelete, sessionv2.VerbObjectDelete, session.VerbObjectDelete
		}

		// another error variable to not shadow err used in defer
		reqMD, metaHdrErr := s.handleRequestMetaHeader(req.MetaHeader, verb, verbV1, cnrID, objID)
		if metaHdrErr != nil {
			err = metaHdrErr // defer
			return s.sendStatusPutResponse(gStream, err, reqFirst)
		}

		if reqInfo, objOwner, err := s.reqInfoProc.PutRequestToInfo(req, initPart, cnrID, op, reqMD.tokens); err != nil {
			if !errors.Is(err, aclsvc.ErrSkipRequest) {
				if !errors.Is(err, apistatus.Error) {
					err = newBadRequestError(err.Error()) // defer
				}
				return s.sendStatusPutResponse(gStream, err, reqFirst)
			}
		} else {
			if !s.aclChecker.CheckBasicACL(reqInfo) || !s.aclChecker.StickyBitCheck(reqInfo, objOwner) {
				err = basicACLErr(reqInfo) // needed for defer
				return s.sendStatusPutResponse(gStream, err, reqFirst)
			}
			err = s.aclChecker.CheckEACL(gStream.Context(), req, cnrID, objID, reqInfo)
			if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
				err = eACLErr(reqInfo, err) // needed for defer
				return s.sendStatusPutResponse(gStream, err, reqFirst)
			}
		}

		if err = ps.forwardInitRequest(req, initPart, reqMD); err != nil {
			err = s.sendStatusPutResponse(gStream, err, reqFirst) // assign for defer
			return err
		}
	}
}

func (s *Server) signDeleteResponse(resp *protoobject.DeleteResponse, err error, req *protoobject.DeleteRequest) *protoobject.DeleteResponse {
	if err != nil {
		resp.MetaHeader = s.makeResponseMetaHeader(util.ToStatus(err))
	}
	resp.VerifyHeader = util.SignResponseIfNeeded(&s.signer, resp, req)
	return resp
}

func (s *Server) makeStatusDeleteResponse(err error, req *protoobject.DeleteRequest) *protoobject.DeleteResponse {
	return s.signDeleteResponse(new(protoobject.DeleteResponse), err, req)
}

type deleteResponseBody protoobject.DeleteResponse_Body

func (x *deleteResponseBody) SetAddress(addr oid.Address) {
	x.Tombstone = addr.ProtoMessage()
}

func (s *Server) Delete(ctx context.Context, req *protoobject.DeleteRequest) (*protoobject.DeleteResponse, error) {
	var (
		err error
		t   = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectDelete, err, t) }()

	if err = icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
		return s.makeStatusDeleteResponse(err, req), nil
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusDeleteResponse(apistatus.ErrNodeUnderMaintenance, req), nil
	}

	body := req.Body
	if body == nil {
		err = newBadRequestError(missingRequestBodyMessage) // defer
		return s.makeStatusDeleteResponse(err, req), nil
	}

	cnrID, objID, err := fetchRequiredObjectAddress(body.Address)
	if err != nil {
		err = newBadRequestError(invalidRequestBodyMessage + ": " + err.Error()) // defer
		return s.makeStatusDeleteResponse(err, req), nil
	}

	reqMD, err := s.handleRequestMetaHeader(req.MetaHeader, sessionv2.VerbObjectDelete, session.VerbObjectDelete, cnrID, objID)
	if err != nil {
		return s.makeStatusDeleteResponse(err, req), nil
	}

	reqInfo, err := s.reqInfoProc.DeleteRequestToInfo(req, cnrID, reqMD.tokens)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			err = newBadRequestError(err.Error()) // defer
		}
		return s.makeStatusDeleteResponse(err, req), nil
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.makeStatusDeleteResponse(err, req), nil
	}
	err = s.aclChecker.CheckEACL(ctx, req, cnrID, objID, reqInfo)
	if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
		err = eACLErr(reqInfo, err) // needed for defer
		return s.makeStatusDeleteResponse(err, req), nil
	}

	cp := objutil.CommonPrmFromRequest(reqMD.ttl, reqMD.xHeaders, reqMD.tokens)

	var rb protoobject.DeleteResponse_Body

	var p deletesvc.Prm
	p.SetCommonParameters(cp)
	p.WithAddress(oid.NewAddress(cnrID, objID))
	p.WithTombstoneAddressTarget((*deleteResponseBody)(&rb))
	err = s.handlers.Delete(ctx, p)
	if err != nil && !errors.Is(err, apistatus.ErrIncomplete) {
		return s.makeStatusDeleteResponse(err, req), nil
	}

	return s.signDeleteResponse(&protoobject.DeleteResponse{Body: &rb}, err, req), nil
}

func (s *Server) signHeadResponse(resp *protoobject.HeadResponse, sign bool) *protoobject.HeadResponse {
	if sign {
		resp.VerifyHeader = util.SignResponse(&s.signer, resp)
	}
	return resp
}

func (s *Server) makeStatusHeadResponse(err error, sign bool) *protoobject.HeadResponse {
	var splitErr *object.SplitInfoError
	if errors.As(err, &splitErr) {
		return s.signHeadResponse(&protoobject.HeadResponse{
			Body: &protoobject.HeadResponse_Body{
				Head: &protoobject.HeadResponse_Body_SplitInfo{
					SplitInfo: splitErr.SplitInfo().ProtoMessage(),
				},
			},
		}, sign)
	}
	return s.signHeadResponse(&protoobject.HeadResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}, sign)
}

// Head implements [protoobject.ObjectServiceServer] so that generated functions
// don't panic. It must never be called, [Server.HeadBuffered] must be used
// instead.
func (s *Server) Head(context.Context, *protoobject.HeadRequest) (*protoobject.HeadResponse, error) {
	panic("must not be called")
}

// HeadBuffered serves req and returns response as either
// [*protoobject.HeadResponse], [mem.BufferSlice] or [mem.Buffer]. All buffers
// must be freed eventually.
func (s *Server) HeadBuffered(ctx context.Context, req *protoobject.HeadRequest) any {
	var (
		err         error
		recheckEACL bool
		t           = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectHead, err, t) }()

	needSignResp := needSignGetResponse(req)

	if err := icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
		return s.makeStatusHeadResponse(err, needSignResp)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusHeadResponse(apistatus.ErrNodeUnderMaintenance, needSignResp)
	}

	body := req.Body
	if body == nil {
		err = newBadRequestError(missingRequestBodyMessage) // defer
		return s.makeStatusHeadResponse(err, needSignResp)
	}

	cnrID, objID, err := fetchRequiredObjectAddress(body.Address)
	if err != nil {
		err = newBadRequestError(invalidRequestBodyMessage + ": " + err.Error()) // defer
		return s.makeStatusHeadResponse(err, needSignResp)
	}

	reqMD, err := s.handleRequestMetaHeader(req.MetaHeader, sessionv2.VerbObjectHead, session.VerbObjectHead, cnrID, objID)
	if err != nil {
		return s.makeStatusHeadResponse(err, needSignResp)
	}

	reqInfo, err := s.reqInfoProc.HeadRequestToInfo(req, cnrID, reqMD.tokens)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			err = newBadRequestError(err.Error()) // defer
		}
		return s.makeStatusHeadResponse(err, needSignResp)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.makeStatusHeadResponse(err, needSignResp)
	}
	err = s.aclChecker.CheckEACL(ctx, req, cnrID, objID, reqInfo)
	if err != nil {
		if !errors.Is(err, aclsvc.ErrNotMatched) {
			err = eACLErr(reqInfo, err) // needed for defer
			return s.makeStatusHeadResponse(err, needSignResp)
		}
		recheckEACL = true
	}

	var resp protoobject.HeadResponse
	p, err := convertHeadPrm(s.signer, reqInfo.Container, req, &resp, cnrID, objID, reqMD)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			err = newBadRequestError(err.Error()) // defer
		}
		return s.makeStatusHeadResponse(err, needSignResp)
	}

	var forwardResp mem.BufferSlice

	p.SetForwardRequestFunc(func(ctx context.Context, node clientcore.MultiAddressClient) error {
		var err error
		forwardResp, err = forwardHeadRequest(ctx, req, node)
		return err
	})

	respMemBuf, hdrBuf := getBufferForHeadResponse()
	defer respMemBuf.Free()

	hdrLen := -1

	p.WithBuffer(hdrBuf, func(ln int) { hdrLen = ln })

	var proxyRespBuf mem.BufferSlice
	var proxyHdrBuf iprotobuf.BuffersSlice
	p.SetSubmitHeadResponseFunc(func(respBuf mem.BufferSlice, hdrBuf iprotobuf.BuffersSlice) {
		proxyRespBuf, proxyHdrBuf = respBuf, hdrBuf
	})

	err = s.handlers.Head(ctx, p)
	if err != nil {
		return s.makeStatusHeadResponse(err, needSignResp)
	}

	if forwardResp != nil {
		return forwardResp
	}

	var buffered bool
	var sigf, hdrf iprotobuf.FieldBounds
	if proxyRespBuf != nil {
		if !recheckEACL || proxyHdrBuf.IsEmpty() {
			return proxyRespBuf
		}
		// TODO: this can be optimized by passing iprotobuf.BuffersSlice into eACL checker
		hdrBuf = proxyHdrBuf.ReadOnlyData()
		hdrf.To = len(hdrBuf)
		buffered = true
	} else if buffered = hdrLen >= 0; buffered {
		_, sigf, hdrf, err = iobject.GetNonPayloadFieldBounds(hdrBuf[:hdrLen])
		if err != nil {
			return s.makeStatusHeadResponse(err, needSignResp)
		}
	}

	if recheckEACL { // previous check didn't match, but we have a header now.
		var msg any
		if buffered {
			msg = hdrBuf[hdrf.ValueFrom:hdrf.To]
		} else {
			msg = &resp
		}
		err = s.aclChecker.CheckEACL(ctx, msg, cnrID, objID, reqInfo)
		if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
			err = eACLErr(reqInfo, err) // defer
			return s.makeStatusHeadResponse(err, needSignResp)
		}

		if proxyRespBuf != nil {
			return proxyRespBuf
		}
	}

	if !buffered {
		return s.signHeadResponse(&resp, needSignResp)
	}

	bodyf := shiftHeaderInHeadResponseBuffer(respMemBuf.SliceBuffer, hdrBuf, sigf, hdrf, needSignResp)
	metaFrom, metaTo := s.writeMetaHeaderToResponseBuffer(respMemBuf.SliceBuffer[bodyf.To:])

	respTo := bodyf.To + metaTo
	if needSignResp {
		var n int
		n, err = s.signResponse(respMemBuf.SliceBuffer[respTo:], respMemBuf.SliceBuffer[bodyf.ValueFrom:bodyf.To], respMemBuf.SliceBuffer[bodyf.To:][metaFrom:metaTo])
		if err != nil {
			err = fmt.Errorf("sign response: %w", err) // defer
			return s.makeStatusHeadResponse(err, needSignResp)
		}
		respTo += n
	}

	respMemBuf.SetBounds(bodyf.From, respTo)
	respMemBuf.Ref()
	return respMemBuf
}

type headResponse struct {
	dst *protoobject.HeadResponse
}

func (x *headResponse) WriteHeader(hdr *object.Object) error {
	mo := hdr.ProtoMessage()
	x.dst.Body = &protoobject.HeadResponse_Body{
		Head: &protoobject.HeadResponse_Body_Header{
			Header: &protoobject.HeaderWithSignature{
				Header:    mo.GetHeader(),
				Signature: mo.GetSignature(),
			},
		},
	}
	return nil
}

// converts original request into parameters accepted by the internal handler.
// Note that the response is untouched within this call.
func convertHeadPrm(signer ecdsa.PrivateKey, cnr container.Container, req *protoobject.HeadRequest, resp *protoobject.HeadResponse, cnrID cid.ID, objID oid.ID, reqMD requestMetadata) (getsvc.HeadPrm, error) {
	cp := objutil.CommonPrmFromRequest(reqMD.ttl, reqMD.xHeaders, reqMD.tokens)

	var p getsvc.HeadPrm
	p.SetCommonParameters(cp)
	p.WithAddress(oid.NewAddress(cnrID, objID))
	p.WithContainer(cnr)
	p.WithRawFlag(req.Body.Raw)
	p.SetHeaderWriter(&headResponse{
		dst: resp,
	})
	if cp.LocalOnly() {
		return p, nil
	}

	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.HeadPrm{}, errors.New("missing meta header")
	}

	var updatedRequest bool

	p.SetTransportFunc(func(ctx context.Context, c clientcore.MultiAddressClient) (mem.BufferSlice, iprotobuf.BuffersSlice, error) {
		if !updatedRequest {
			updatedRequest = true
			req = &protoobject.HeadRequest{
				Body: req.Body,
				MetaHeader: &protosession.RequestMetaHeader{
					Version: version.Current().ProtoMessage(),
					Ttl:     1,
				},
			}
			var err error
			req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(neofsecdsa.Signer(signer), req, nil)
			if err != nil {
				return nil, iprotobuf.BuffersSlice{}, err
			}
		}

		var respBuf mem.BufferSlice
		var hdr iprotobuf.BuffersSlice
		return respBuf, hdr, c.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			var err error
			respBuf, hdr, err = getHeaderFromRemoteNode(ctx, conn, req, objID)
			return err // TODO: log error
		})
	})
	return p, nil
}

// GetRangeHash is deprecated and no longer supported by the node.
func (s *Server) GetRangeHash(_ context.Context, _ *protoobject.GetRangeHashRequest) (*protoobject.GetRangeHashResponse, error) {
	return nil, grpcstatus.Error(grpccodes.Unimplemented, "no longer supported")
}

func (s *Server) sendGetResponse(stream protoobject.ObjectService_GetServer, resp *protoobject.GetResponse, sign bool) error {
	if sign {
		resp.VerifyHeader = util.SignResponse(&s.signer, resp)
	}
	return stream.Send(resp)
}

func (s *Server) sendStatusGetResponse(stream protoobject.ObjectService_GetServer, err error, sign bool) error {
	var splitErr *object.SplitInfoError
	if errors.As(err, &splitErr) {
		return s.sendGetResponse(stream, &protoobject.GetResponse{
			Body: &protoobject.GetResponse_Body{
				ObjectPart: &protoobject.GetResponse_Body_SplitInfo{
					SplitInfo: splitErr.SplitInfo().ProtoMessage(),
				},
			},
		}, sign)
	}
	return s.sendGetResponse(stream, &protoobject.GetResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}, sign)
}

type getStream struct {
	base    protoobject.ObjectService_GetServer
	srv     *Server
	reqCID  cid.ID
	reqOID  oid.ID
	reqInfo aclsvc.RequestInfo

	recheckEACL  bool
	signResponse bool
	payloadOnly  bool
}

func (s *getStream) ValidateHeader(hdr *object.Object) error {
	if !s.recheckEACL {
		return nil
	}

	mo := hdr.ProtoMessage()
	resp := &protoobject.GetResponse{
		Body: &protoobject.GetResponse_Body{
			ObjectPart: &protoobject.GetResponse_Body_Init_{Init: &protoobject.GetResponse_Body_Init{
				ObjectId:  mo.ObjectId,
				Signature: mo.Signature,
				Header:    mo.Header,
			}},
		},
	}

	err := s.srv.aclChecker.CheckEACL(s.base.Context(), resp, s.reqCID, s.reqOID, s.reqInfo)
	if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
		return eACLErr(s.reqInfo, err)
	}
	return nil
}

func (s *getStream) WriteHeader(hdr *object.Object) error {
	if err := s.ValidateHeader(hdr); err != nil {
		return err
	}
	if s.payloadOnly {
		return nil
	}

	mo := hdr.ProtoMessage()
	resp := &protoobject.GetResponse{
		Body: &protoobject.GetResponse_Body{
			ObjectPart: &protoobject.GetResponse_Body_Init_{Init: &protoobject.GetResponse_Body_Init{
				ObjectId:  mo.ObjectId,
				Signature: mo.Signature,
				Header:    mo.Header,
			}},
		},
	}
	return s.srv.sendGetResponse(s.base, resp, s.signResponse)
}

func (s *getStream) WriteChunk(chunk []byte) error {
	for buf := bytes.NewBuffer(chunk); buf.Len() > 0; {
		newResp := &protoobject.GetResponse{
			Body: &protoobject.GetResponse_Body{
				ObjectPart: &protoobject.GetResponse_Body_Chunk{
					Chunk: buf.Next(maxRespDataChunkSize),
				},
			},
		}
		if err := s.srv.sendGetResponse(s.base, newResp, s.signResponse); err != nil {
			return err
		}
	}
	s.srv.metrics.AddGetPayload(len(chunk))
	return nil
}

func (s *Server) Get(req *protoobject.GetRequest, gStream protoobject.ObjectService_GetServer) error {
	var (
		err         error
		recheckEACL bool
		t           = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectGet, err, t) }()

	needSignResp := needSignGetResponse(req)

	if err = icrypto.VerifyRequestSignatures(req); err != nil {
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusGetResponse(gStream, apistatus.ErrNodeUnderMaintenance, needSignResp)
	}

	body := req.Body
	if body == nil {
		err = newBadRequestError(missingRequestBodyMessage) // defer
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	cnrID, objID, err := fetchRequiredObjectAddress(body.Address)
	if err != nil {
		err = newBadRequestError(invalidRequestBodyMessage + ": " + err.Error()) // defer
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	reqMD, err := s.handleRequestMetaHeader(req.MetaHeader, sessionv2.VerbObjectGet, session.VerbObjectGet, cnrID, objID)
	if err != nil {
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	reqInfo, err := s.reqInfoProc.GetRequestToInfo(req, cnrID, reqMD.tokens)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			err = newBadRequestError(err.Error()) // defer
		}
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}
	err = s.aclChecker.CheckEACL(gStream.Context(), req, cnrID, objID, reqInfo)
	if err != nil {
		if !errors.Is(err, aclsvc.ErrNotMatched) {
			err = eACLErr(reqInfo, err) // needed for defer
			return s.sendStatusGetResponse(gStream, err, needSignResp)
		}
		recheckEACL = true
	}

	p, err := convertGetPrm(s.signer, reqInfo.Container, req, &getStream{
		base:         gStream,
		srv:          s,
		reqCID:       cnrID,
		reqOID:       objID,
		reqInfo:      reqInfo,
		recheckEACL:  recheckEACL,
		signResponse: needSignResp,
		payloadOnly:  req.GetBody().GetPayloadOnly(),
	}, cnrID, objID, reqMD)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			err = newBadRequestError(err.Error()) // defer
		}
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	p.SetForwardRequestFunc(func(ctx context.Context, node clientcore.MultiAddressClient) error {
		return forwardGetRequest(ctx, req, gStream, node)
	})

	p.WithECTransport(&getECTransport{
		server:                       s,
		requestSessionTokenMessage:   reqMD.sessionTokenMessage,
		requestSessionV1TokenMessage: reqMD.sessionV1TokenMessage,
		requestContainer:             cnrID,
		requestObject:                objID,
		signResponses:                needSignResp,
		responseStream:               gStream,
	})

	// TODO: consider optimization
	// We could acquire ~256K buffer (like for chunks) if storage would try to read it full.
	// Then small objects would fit into a single buffer, and for large ones it'd be possible to
	// encode the first chunk response using the heading buffer.
	var hdrRespBuf *iprotobuf.MemBuffer
	var hdrBuf []byte
	if p.Range() == nil && !p.PayloadOnly() {
		hdrRespBuf, hdrBuf = getBufferForHeadResponse()
		defer hdrRespBuf.Free()
	}

	hdrLen := -1
	var stream io.ReadCloser
	defer func() {
		if stream != nil {
			stream.Close()
		}
	}()

	if hdrBuf != nil {
		p.WithBuffer(hdrBuf, func(ln int, s io.ReadCloser) { hdrLen, stream = ln, s })
	}

	err = s.handlers.Get(gStream.Context(), p)
	if err != nil {
		if errors.Is(err, getsvc.ErrResponseStreamFailure) {
			return err
		}
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	if hdrLen < 0 {
		return nil
	}

	idf, sigf, hdrf, err := iobject.GetNonPayloadFieldBounds(hdrBuf[:hdrLen])
	if err != nil {
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	if recheckEACL { // previous check didn't match, but we have a header now.
		err = s.aclChecker.CheckEACL(gStream.Context(), hdrBuf[hdrf.ValueFrom:hdrf.To], cnrID, objID, reqInfo)
		if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
			err = eACLErr(reqInfo, err) // defer
			return s.sendStatusGetResponse(gStream, err, needSignResp)
		}
	}

	pldFldOff := max(idf.To, sigf.To, hdrf.To)

	err = s.copyGetStream(gStream, hdrRespBuf, hdrBuf, hdrLen, pldFldOff, stream, pldFldOff, needSignResp) // defer
	if err != nil {
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	return nil
}

func (s *Server) copyGetStream(gStream grpc.ServerStream, hdrRespBuf *iprotobuf.MemBuffer, hdrBuf []byte,
	prefixLen, hdrTo int, stream io.Reader, pldFldOff int, needSignResp bool) error {
	var chunkRespBuf *iprotobuf.MemBuffer
	var chunkBuf []byte

	prereadPldLen := prefixLen - pldFldOff
	if prereadPldLen > 0 {
		chunkRespBuf, chunkBuf = getBufferForChunkGetResponse()
		copy(chunkBuf, hdrBuf[pldFldOff:][:prereadPldLen])
		// TODO: consider optimization
		// Object can be small and fit entirely within the heading buffer. In this
		// case, no more buffers are needed. This can be checked by reading
		// `header.payload_length` field.
	}

	bodyf := shiftHeaderInGetResponseBuffer(hdrRespBuf.SliceBuffer, hdrBuf[:hdrTo])

	if needSignResp {
		n, err := s.signResponse(hdrRespBuf.SliceBuffer[bodyf.To:], hdrRespBuf.SliceBuffer[bodyf.ValueFrom:bodyf.To], nil)
		if err != nil {
			if chunkRespBuf != nil {
				chunkRespBuf.Free()
			}
			return fmt.Errorf("sign head response: %w", err)
		}
		bodyf.To += n
	}

	hdrRespBuf.SetBounds(bodyf.From, bodyf.To)
	hdrRespBuf.Ref() // because Free() is defered
	// Note that finished SendMsg() does not guarantee that the buffer is free.
	// Moreover, this is not guaranteed even by returning from the current function.
	// Therefore, buffer release has to be delegated to gRPC layer.
	// For the same reason, reusing a single buffer for multiple messages is unsafe.
	if err := gStream.SendMsg(hdrRespBuf); err != nil {
		if chunkRespBuf != nil {
			chunkRespBuf.Free()
		}
		return fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
	}

	if chunkRespBuf == nil {
		chunkRespBuf, chunkBuf = getBufferForChunkGetResponse()
	}

	var sent int
	defer func() {
		if sent > 0 {
			s.metrics.AddGetPayload(sent)
		}
	}()

	for first := true; ; first = false {
		n, err := io.ReadFull(stream, chunkBuf[prereadPldLen:])
		streamDone := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		if err != nil && !streamDone {
			chunkRespBuf.Free()
			return copyReadError{
				error:   fmt.Errorf("read payload stream: %w", err),
				written: sent,
			}
		}

		n += prereadPldLen

		if first && n > 0 {
			prereadPldLen, err = parseObjectPayloadFieldTag(chunkBuf[:n])
			if err != nil {
				chunkRespBuf.Free()
				return fmt.Errorf("parse payload field tag: %w", err)
			}

			n -= prereadPldLen
			bodyf = shiftPayloadChunkInGetResponseBuffer(chunkRespBuf.SliceBuffer, maxChunkOffsetInGetResponse+prereadPldLen, n)
			prereadPldLen = 0
		} else if n == 0 {
			chunkRespBuf.Free()
			return nil
		} else {
			bodyf = shiftPayloadChunkInGetResponseBuffer(chunkRespBuf.SliceBuffer, maxChunkOffsetInGetResponse, n)
		}

		if needSignResp {
			n, err := s.signResponse(chunkRespBuf.SliceBuffer[bodyf.To:], chunkRespBuf.SliceBuffer[bodyf.ValueFrom:bodyf.To], nil)
			if err != nil {
				chunkRespBuf.Free()
				return fmt.Errorf("sign chunk response: %w", err)
			}
			bodyf.To += n
		}

		chunkRespBuf.SetBounds(bodyf.From, bodyf.To)
		if err = gStream.SendMsg(chunkRespBuf); err != nil {
			return fmt.Errorf("%w: %w", getsvc.ErrResponseStreamFailure, err)
		}
		sent += n
		if streamDone {
			return nil
		}

		chunkRespBuf, chunkBuf = getBufferForChunkGetResponse()
	}
}

// converts original request into parameters accepted by the internal handler.
// Note that the stream is untouched within this call, errors are not reported
// into it.
func convertGetPrm(signer ecdsa.PrivateKey, cnr container.Container, req *protoobject.GetRequest, stream *getStream, cnrID cid.ID, objID oid.ID, reqMD requestMetadata) (getsvc.Prm, error) {
	body := req.GetBody()

	rng := body.GetRange()
	if rng != nil {
		rln := rng.GetLength()
		if rln == 0 {
			if rng.GetOffset() != 0 {
				return getsvc.Prm{}, errors.New("zero range length")
			}
		} else if rng.GetOffset()+rln <= rng.GetOffset() {
			return getsvc.Prm{}, errors.New("range overflow")
		}
	}

	cp := objutil.CommonPrmFromRequest(reqMD.ttl, reqMD.xHeaders, reqMD.tokens)

	var p getsvc.Prm
	p.SetCommonParameters(cp)
	p.WithAddress(oid.NewAddress(cnrID, objID))
	p.WithContainer(cnr)
	p.WithRawFlag(body.Raw)
	p.SetObjectWriter(stream)
	if rng != nil {
		var objRng object.Range
		objRng.SetOffset(rng.GetOffset())
		objRng.SetLength(rng.GetLength())
		p.SetRange(&objRng)
	}
	if body.GetPayloadOnly() {
		p.MarkPayloadOnly()
	}
	if stream.recheckEACL {
		p.RequireEACLRecheck()
	}
	if cp.LocalOnly() {
		return p, nil
	}

	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.Prm{}, errors.New("missing meta header")
	}

	proxyCtx := getProxyContext{
		respStream:   stream,
		suppressInit: body.GetPayloadOnly(),
	}

	var updatedRequest bool

	p.SetTransportFunc(func(ctx context.Context, c clientcore.MultiAddressClient) error {
		if !updatedRequest {
			updatedRequest = true

			req = &protoobject.GetRequest{
				Body: req.Body,
				MetaHeader: &protosession.RequestMetaHeader{
					Version: version.Current().ProtoMessage(),
					Ttl:     1,
				},
			}
			if proxyCtx.suppressInit {
				req.Body.PayloadOnly = false
			}
			var err error
			req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(neofsecdsa.Signer(signer), req, nil)
			if err != nil {
				return err
			}
		}

		return c.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			return proxyCtx.continueWithConn(ctx, req, conn) // TODO: log error
		})
	})
	return p, nil
}

type getProxyContext struct {
	respStream   *getStream
	suppressInit bool

	onceHdr sync.Once

	payloadLenCheck  uint64
	payloadHashCheck []byte

	respondedPayload int
	payloadHashGot   hash.Hash
}

func (s *Server) sendRangeResponse(stream protoobject.ObjectService_GetRangeServer, resp *protoobject.GetRangeResponse, req *protoobject.GetRangeRequest) error {
	resp.VerifyHeader = util.SignResponseIfNeeded(&s.signer, resp, req)
	return stream.Send(resp)
}

func (s *Server) sendStatusRangeResponse(stream protoobject.ObjectService_GetRangeServer, err error, req *protoobject.GetRangeRequest) error {
	var splitErr *object.SplitInfoError
	if errors.As(err, &splitErr) {
		return s.sendRangeResponse(stream, &protoobject.GetRangeResponse{
			Body: &protoobject.GetRangeResponse_Body{
				RangePart: &protoobject.GetRangeResponse_Body_SplitInfo{
					SplitInfo: splitErr.SplitInfo().ProtoMessage(),
				},
			},
		}, req)
	}
	return s.sendRangeResponse(stream, &protoobject.GetRangeResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}, req)
}

type rangeStream struct {
	base protoobject.ObjectService_GetRangeServer
	srv  *Server
	req  *protoobject.GetRangeRequest

	respondedPayload int

	signResponse bool
}

func (s *rangeStream) WriteChunk(chunk []byte) error {
	for buf := bytes.NewBuffer(chunk); buf.Len() > 0; {
		newResp := &protoobject.GetRangeResponse{
			Body: &protoobject.GetRangeResponse_Body{
				RangePart: &protoobject.GetRangeResponse_Body_Chunk{
					Chunk: buf.Next(maxRespDataChunkSize),
				},
			},
		}
		if err := s.srv.sendRangeResponse(s.base, newResp, s.req); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) GetRange(req *protoobject.GetRangeRequest, gStream protoobject.ObjectService_GetRangeServer) error {
	var (
		err error
		t   = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectRange, err, t) }()
	if err = icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
		return s.sendStatusRangeResponse(gStream, err, req)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusRangeResponse(gStream, apistatus.ErrNodeUnderMaintenance, req)
	}

	body := req.Body
	if body == nil {
		err = newBadRequestError(missingRequestBodyMessage) // defer
		return s.sendStatusRangeResponse(gStream, err, req)
	}

	cnrID, objID, err := fetchRequiredObjectAddress(body.Address)
	if err != nil {
		err = newBadRequestError(invalidRequestBodyMessage + ": " + err.Error()) // defer
		return s.sendStatusRangeResponse(gStream, err, req)
	}

	reqMD, err := s.handleRequestMetaHeader(req.MetaHeader, sessionv2.VerbObjectRange, session.VerbObjectRange, cnrID, objID)
	if err != nil {
		return s.sendStatusRangeResponse(gStream, err, req)
	}

	reqInfo, err := s.reqInfoProc.RangeRequestToInfo(req, cnrID, reqMD.tokens)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			err = newBadRequestError(err.Error()) // defer
		}
		return s.sendStatusRangeResponse(gStream, err, req)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.sendStatusRangeResponse(gStream, err, req)
	}
	err = s.aclChecker.CheckEACL(gStream.Context(), req, cnrID, objID, reqInfo)
	if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
		err = eACLErr(reqInfo, err) // needed for defer
		return s.sendStatusRangeResponse(gStream, err, req)
	}

	needSignResponse := needSignGetResponse(req)

	p, err := convertRangePrm(s.signer, reqInfo.Container, req, &rangeStream{
		base:         gStream,
		srv:          s,
		req:          req,
		signResponse: needSignResponse,
	}, cnrID, objID, reqMD)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			err = newBadRequestError(err.Error()) // defer
		}
		return s.sendStatusRangeResponse(gStream, err, req)
	}

	p.SetForwardRequestFunc(func(ctx context.Context, node clientcore.MultiAddressClient) error {
		return forwardRangeRequest(ctx, req, gStream, node)
	})

	var stream io.ReadCloser
	defer func() {
		if stream != nil {
			stream.Close()
		}
	}()

	hdrRespBuf, hdrBuf := getBufferForHeadResponse()
	defer hdrRespBuf.Free()

	p.WithBuffer(hdrBuf, func(s io.ReadCloser) { stream = s })

	err = s.handlers.GetRange(gStream.Context(), p)
	if err != nil {
		return s.sendStatusRangeResponse(gStream, err, req)
	}

	if stream == nil {
		return nil
	}

	err = s.copyRangeStream(gStream, stream, needSignResponse, shiftPayloadChunkInRangeResponseBuffer)
	if err != nil {
		return s.sendStatusRangeResponse(gStream, err, req)
	}

	return nil
}

func (s *Server) copyRangeStream(gStream grpc.ServerStream, stream io.Reader, needSignResp bool, shiftFn func([]byte, int, int) iprotobuf.FieldBounds) error {
	var sent int
	for {
		// chunk response buffers for GET completely suitable for RANGE
		respBuf, buf := getBufferForChunkGetResponse()

		n, err := io.ReadFull(stream, buf)
		streamDone := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		if err != nil && !streamDone {
			respBuf.Free()
			return copyReadError{
				error:   fmt.Errorf("read payload stream: %w", err),
				written: sent,
			}
		}

		if n == 0 {
			respBuf.Free()
			return nil
		}

		bodyf := shiftFn(respBuf.SliceBuffer, maxChunkOffsetInGetResponse, n)

		if needSignResp {
			n, err := s.signResponse(respBuf.SliceBuffer[bodyf.To:], respBuf.SliceBuffer[bodyf.ValueFrom:bodyf.To], nil)
			if err != nil {
				respBuf.Free()
				return fmt.Errorf("sign chunk response: %w", err)
			}
			bodyf.To += n
		}

		respBuf.SetBounds(bodyf.From, bodyf.To)
		if err = gStream.SendMsg(respBuf); err != nil {
			return err
		}

		sent += n

		if streamDone {
			return nil
		}
	}
}

// converts original request into parameters accepted by the internal handler.
// Note that the stream is untouched within this call, errors are not reported
// into it.
func convertRangePrm(signer ecdsa.PrivateKey, cnr container.Container, req *protoobject.GetRangeRequest, stream *rangeStream, cnrID cid.ID, objID oid.ID, reqMD requestMetadata) (getsvc.RangePrm, error) {
	body := req.GetBody()

	rln := body.Range.GetLength()
	if rln == 0 { // includes nil range
		if body.Range.Offset != 0 {
			return getsvc.RangePrm{}, errors.New("zero range length")
		} // else whole payload
	} else if body.Range.Offset+rln <= body.Range.Offset {
		return getsvc.RangePrm{}, errors.New("range overflow")
	}

	cp := objutil.CommonPrmFromRequest(reqMD.ttl, reqMD.xHeaders, reqMD.tokens)

	var p getsvc.RangePrm
	p.SetCommonParameters(cp)
	p.WithAddress(oid.NewAddress(cnrID, objID))
	p.WithContainer(cnr)
	p.WithRawFlag(body.Raw)
	p.SetChunkWriter(stream)
	var rng object.Range
	rng.SetOffset(body.Range.Offset)
	rng.SetLength(rln)
	p.SetRange(&rng)
	if cp.LocalOnly() {
		return p, nil
	}

	var onceResign sync.Once
	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.RangePrm{}, errors.New("missing meta header")
	}
	p.SetTransportFunc(func(ctx context.Context, c clientcore.MultiAddressClient) error {
		var err error
		onceResign.Do(func() {
			req.MetaHeader = &protosession.RequestMetaHeader{
				// TODO: #1165 think how to set the other fields
				Ttl:    meta.GetTtl() - 1,
				Origin: meta,
			}
			req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(neofsecdsa.Signer(signer), req, nil)
		})
		if err != nil {
			return err
		}

		return c.ForAnyGRPCConn(ctx, stream.continueWithConn)
	})
	return p, nil
}

func (s *Server) Search(_ *protoobject.SearchRequest, _ protoobject.ObjectService_SearchServer) error {
	return grpcstatus.Error(grpccodes.Unimplemented, "no longer supported, use SearchV2")
}

// Replicate serves neo.fs.v2.object.ObjectService/Replicate RPC.
func (s *Server) Replicate(ctx context.Context, req *protoobject.ReplicateRequest) (*protoobject.ReplicateResponse, error) {
	if req.Object == nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeBadRequest, Message: "binary object field is missing/empty",
		}}, nil
	}

	if req.Object.ObjectId == nil || len(req.Object.ObjectId.Value) == 0 {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeBadRequest, Message: "ID field is missing/empty in the object field",
		}}, nil
	}

	if req.Signature == nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeBadRequest, Message: "missing object signature field",
		}}, nil
	}

	if len(req.Signature.Key) == 0 {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeBadRequest, Message: "public key field is missing/empty in the object signature field",
		}}, nil
	}

	if len(req.Signature.Sign) == 0 {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code: codeBadRequest, Message: "signature value is missing/empty in the object signature field",
		}}, nil
	}

	switch scheme := req.Signature.Scheme; scheme {
	default:
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeBadRequest,
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
			Code:    codeBadRequest,
			Message: "missing header field in the object field",
		}}, nil
	}

	gCnrMsg := hdr.GetContainerId()
	if gCnrMsg == nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeBadRequest,
			Message: "missing container ID field in the object header field",
		}}, nil
	}

	var cnr cid.ID
	err := cnr.FromProtoMessage(gCnrMsg)
	if err != nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeBadRequest,
			Message: fmt.Sprintf("invalid container ID in the object header field: %v", err),
		}}, nil
	}

	var pubKey neofscrypto.PublicKey
	switch req.Signature.Scheme { //nolint:exhaustive
	// other cases already checked above
	case refs.SignatureScheme_ECDSA_SHA512:
		pubKey = new(neofsecdsa.PublicKey)
		err = pubKey.Decode(req.Signature.Key)
		if err != nil {
			return &protoobject.ReplicateResponse{Status: &protostatus.Status{
				Code:    codeBadRequest,
				Message: "invalid ECDSA public key in the object signature field",
			}}, nil
		}
	case refs.SignatureScheme_ECDSA_RFC6979_SHA256:
		pubKey = new(neofsecdsa.PublicKeyRFC6979)
		err = pubKey.Decode(req.Signature.Key)
		if err != nil {
			return &protoobject.ReplicateResponse{Status: &protostatus.Status{
				Code:    codeBadRequest,
				Message: "invalid ECDSA public key in the object signature field",
			}}, nil
		}
	case refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT:
		pubKey = new(neofsecdsa.PublicKeyWalletConnect)
		err = pubKey.Decode(req.Signature.Key)
		if err != nil {
			return &protoobject.ReplicateResponse{Status: &protostatus.Status{
				Code:    codeBadRequest,
				Message: "invalid ECDSA public key in the object signature field",
			}}, nil
		}
	}
	if !pubKey.Verify(req.Object.ObjectId.Value, req.Signature.Sign) {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeBadRequest,
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
			Code:    codeBadRequest,
			Message: fmt.Sprintf("invalid object field: %v", err),
		}}, nil
	}

	err = s.storage.VerifyAndStoreObjectLocally(ctx, *obj)
	if err != nil {
		if errors.Is(err, apistatus.ErrBusy) {
			return &protoobject.ReplicateResponse{Status: apistatus.FromError(err)}, nil
		}
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

func (s *Server) signSearchResponse(body *protoobject.SearchV2Response_Body, err error, req *protoobject.SearchV2Request) *protoobject.SearchV2Response {
	var resp = new(protoobject.SearchV2Response)

	if err != nil {
		resp.MetaHeader = s.makeResponseMetaHeader(apistatus.FromError(err))
	}
	if err == nil || errors.Is(err, apistatus.ErrIncomplete) {
		resp.Body = body
	}
	resp.VerifyHeader = util.SignResponseIfNeeded(&s.signer, resp, req)
	return resp
}

// SearchV2 implements [protoobject.ObjectServiceServer] so that generated
// functions don't panic. It must never be called, [Server.SearchV2Buffered]
// must be used instead.
func (s *Server) SearchV2(context.Context, *protoobject.SearchV2Request) (*protoobject.SearchV2Response, error) {
	panic("must not be called")
}

// SearchV2Buffered serves req and returns response as either
// [*protoobject.SearchV2Response], [mem.BufferSlice] or [mem.Buffer]. All
// buffers must be freed eventually.
func (s *Server) SearchV2Buffered(ctx context.Context, req *protoobject.SearchV2Request) any {
	var (
		err error
		t   = time.Now()
	)
	defer s.pushOpExecResult(stat.MethodObjectSearchV2, err, t)
	if err = icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
		return s.signSearchResponse(nil, err, req)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.signSearchResponse(nil, apistatus.ErrNodeUnderMaintenance, req)
	}

	body := req.Body
	if body == nil {
		err = newBadRequestError(missingRequestBodyMessage) // defer
		return s.signSearchResponse(nil, err, req)
	}

	cnrID, err := fetchRequiredContainerID(body.ContainerId)
	if err != nil {
		err = newBadRequestError(invalidRequestBodyMessage + ": " + err.Error()) // defer
		return s.signSearchResponse(nil, err, req)
	}

	reqMD, err := s.handleRequestMetaHeader(req.MetaHeader, sessionv2.VerbObjectRange, session.VerbObjectRange, cnrID, oid.ID{})
	if err != nil {
		return s.signSearchResponse(nil, err, req)
	}

	reqInfo, err := s.reqInfoProc.SearchV2RequestToInfo(req, cnrID, reqMD.tokens)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			err = newBadRequestError(err.Error()) // defer
		}
		return s.signSearchResponse(nil, err, req)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.signSearchResponse(nil, err, req)
	}
	err = s.aclChecker.CheckEACL(ctx, req, cnrID, oid.ID{}, reqInfo)
	if err != nil && !errors.Is(err, aclsvc.ErrNotMatched) { // Not matched -> follow basic ACL.
		err = eACLErr(reqInfo, err)
		return s.signSearchResponse(nil, err, req)
	}

	respBody, err := s.processSearchRequest(ctx, req, cnrID)

	var respBufErr igrpc.MemBufferSliceError
	if errors.As(err, &respBufErr) {
		return mem.BufferSlice(respBufErr)
	}

	return s.signSearchResponse(respBody, err, req)
}

func verifySearchFilter(f *protoobject.SearchFilter) error {
	switch f.Key {
	case "":
		return errors.New("missing attribute")
	case object.FilterPayloadHomomorphicHash:
		return fmt.Errorf("%s filter target is prohibited starting from API 2.23", object.FilterPayloadHomomorphicHash)
	case object.FilterContainerID, object.FilterID:
		return fmt.Errorf("prohibited attribute %s", f.Key)
	case object.FilterRoot, object.FilterPhysical:
		if f.MatchType != 0 {
			return fmt.Errorf("non-zero matcher %s for attribute %s", f.MatchType, f.Key)
		}
		if f.Value != "" {
			return fmt.Errorf("value for attribute %s is prohibited", f.Key)
		}
	}
	return nil
}

func (s *Server) processSearchRequest(ctx context.Context, req *protoobject.SearchV2Request, cnrID cid.ID) (*protoobject.SearchV2Response_Body, error) {
	body := req.Body
	if body.Version != 1 {
		return nil, errors.New("unsupported query version")
	}
	if body.Count == 0 {
		return nil, errors.New("zero count")
	} else if body.Count > 1000 {
		return nil, fmt.Errorf("number of attributes %d exceeds the limit 1000", body.Count)
	}
	if len(body.Filters) > 8 {
		return nil, fmt.Errorf("number of filter %d exceeds the limit 8", len(body.Filters))
	}
	if len(body.Attributes) > 8 {
		return nil, fmt.Errorf("number of attributes %d exceeds the limit 8", len(body.Attributes))
	}
	for i := range body.Filters {
		if err := verifySearchFilter(body.Filters[i]); err != nil {
			return nil, fmt.Errorf("invalid filter #%d: %w", i, err)
		}
	}
	for i := range body.Attributes {
		if body.Attributes[i] == "" {
			return nil, fmt.Errorf("empty attribute #%d", i)
		}
		if body.Attributes[i] == object.FilterContainerID || body.Attributes[i] == object.FilterID {
			return nil, fmt.Errorf("prohibited attribute %s", body.Attributes[i])
		}
	}
	if len(body.Attributes) > 0 && (len(body.Filters) == 0 || body.Filters[0].Key != body.Attributes[0]) {
		return nil, errors.New("primary attribute must be filtered 1st")
	}

	ttl := req.MetaHeader.GetTtl()
	if ttl == 0 {
		return nil, errors.New("zero TTL")
	}

	res, newCursor, err := s.ProcessSearch(ctx, req, ttl == 1, true, cnrID)
	if err != nil && !errors.Is(err, apistatus.ErrIncomplete) {
		return nil, err
	}

	resBody := &protoobject.SearchV2Response_Body{
		Result: make([]*protoobject.SearchV2Response_OIDWithMeta, len(res)),
	}
	for i := range res {
		resBody.Result[i] = &protoobject.SearchV2Response_OIDWithMeta{
			Id:         res[i].ID.ProtoMessage(),
			Attributes: res[i].Attributes,
		}
	}
	if newCursor != nil {
		resBody.Cursor = base64.StdEncoding.EncodeToString(newCursor)
	}
	return resBody, err
}

func (s *Server) ProcessSearch(ctx context.Context, req *protoobject.SearchV2Request, localOnly bool, forwardedResponseToError bool, cID cid.ID) ([]client.SearchResultItem, []byte, error) {
	body := req.GetBody()
	var fs object.SearchFilters
	if err := fs.FromProtoMessage(body.Filters); err != nil {
		return nil, nil, fmt.Errorf("invalid filters: %w", err)
	}

	filteredAttributeless := len(body.Attributes) == 0 && len(body.Filters) > 0
	attrs := body.Attributes
	if filteredAttributeless {
		// 1st attribute is required for merge function to provide proper paging. This
		// requires collecting attribute values from other nodes. Therefore, if the
		// attribute is not requested, it is forcefully returned for local queries, and
		// will end up in resulting cursor.
		attrs = []string{body.Filters[0].Key}
	}
	ofs, cursor, err := objectcore.PreprocessSearchQuery(fs, attrs, body.Cursor)
	if err != nil {
		if errors.Is(err, objectcore.ErrUnreachableQuery) {
			return nil, nil, nil
		}
		return nil, nil, newBadRequestError(err.Error())
	}

	cnr, err := s.fsChain.Get(cID)
	if err != nil {
		return nil, nil, fmt.Errorf("fetching container: %w", err)
	}
	var handleWithMetaService bool
	const metaOnChainAttr = "__NEOFS__METAINFO_CONSISTENCY"
	switch cnr.Attribute(metaOnChainAttr) {
	case "optimistic", "strict":
		handleWithMetaService = true
	default:
	}

	var (
		count      = uint16(body.Count) // legit according to the limit
		incomplete error
		newCursor  []byte
		res        []client.SearchResultItem
	)
	switch {
	case localOnly:
		if res, newCursor, err = s.storage.SearchObjects(ctx, cID, ofs, attrs, cursor, count); err != nil {
			return nil, nil, err
		}
	case handleWithMetaService:
		res, newCursor, err = s.meta.Search(cID, ofs, attrs, cursor, count)
		if err != nil {
			return nil, nil, err
		}
	default:
		nodeSets, repRules, ecRules, err := s.fsChain.SelectContainerNodes(cID)
		if err != nil {
			return nil, nil, err
		}

		if forwardedResponseToError && !inetmap.NodeSetsContainPublicKeyFunc(nodeSets, s.fsChain.IsOwnPublicKey) {
			respBuf, err := s.forwardSearchRequest(ctx, req, nodeSets)
			if err != nil {
				return nil, nil, err
			}
			return nil, nil, igrpc.MemBufferSliceError(respBuf)
		}

		type nodeSearchResult struct {
			set  []client.SearchResultItem
			more bool
			err  error
		}
		var (
			mores           []bool
			mProcessedNodes = make(map[string]struct{})
			poolErr         error
			resCh           = make(chan nodeSearchResult)
			sets            [][]client.SearchResultItem
			expectedRes     int
		)

		req = &protoobject.SearchV2Request{
			Body: req.Body,
			MetaHeader: &protosession.RequestMetaHeader{
				Version: version.Current().ProtoMessage(),
				Ttl:     1,
			},
		}
		if req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer[*protoobject.SearchV2Request_Body](neofsecdsa.Signer(s.signer), req, nil); err != nil {
			return nil, nil, fmt.Errorf("sign request: %w", err)
		}

		var optimizedNodes = (len(body.Filters) != 0) &&
			slices.ContainsFunc(body.Filters, func(filt *protoobject.SearchFilter) bool {
				return !strings.HasPrefix(filt.Key, "$Object:") && !strings.HasPrefix(filt.Key, "__NEOFS__")
			})

		iterateSearchableContainerNodes(nodeSets, repRules, ecRules, !optimizedNodes, func(node netmap.NodeInfo) bool {
			nodePub := node.PublicKey()
			strKey := string(nodePub)
			if _, ok := mProcessedNodes[strKey]; ok {
				return true
			}
			mProcessedNodes[strKey] = struct{}{}
			expectedRes++
			if s.fsChain.IsOwnPublicKey(nodePub) {
				go func() {
					set, crsr, err := s.storage.SearchObjects(ctx, cID, ofs, attrs, cursor, count)
					resCh <- nodeSearchResult{set, crsr != nil, err}
				}()
				return true
			}
			if poolErr = s.searchWorkers.Submit(func() {
				set, more, err := s.searchOnRemoteNode(ctx, node, req)
				resCh <- nodeSearchResult{set, more, err}
			}); poolErr != nil {
				expectedRes--
			}
			return poolErr == nil
		})
		for range expectedRes { // Always wait for all threads started above.
			searchRes := <-resCh
			if poolErr != nil {
				continue
			}
			if searchRes.err != nil {
				var inc = new(apistatus.Incomplete)
				inc.SetMessage(fmt.Sprintf("last error: %s", searchRes.err.Error()))
				incomplete = inc
				continue
			}
			sets = append(sets, searchRes.set)
			mores = append(mores, searchRes.more)
		}
		close(resCh)
		if poolErr != nil {
			if errors.Is(poolErr, ants.ErrPoolOverload) {
				var busy = new(apistatus.Busy)
				busy.SetMessage(poolErr.Error())
				return nil, nil, busy
			}
			return nil, nil, poolErr
		}
		var (
			firstAttr   string
			firstFilter *object.SearchFilter
		)
		if len(attrs) > 0 {
			firstFilter = &ofs[0].SearchFilter
			// No reason to compare equal values.
			if body.Filters[0].MatchType != protoobject.MatchType_STRING_EQUAL {
				firstAttr = fs[0].Header()
			}
		}
		cmpInt := firstAttr != "" && objectcore.IsIntegerSearchOp(fs[0].Operation())
		var more bool
		if res, more, err = objectcore.MergeSearchResults(count, firstAttr, cmpInt, sets, mores); err != nil {
			return nil, nil, fmt.Errorf("merge results from container nodes: %w", err)
		}
		if more {
			if filteredAttributeless && body.Filters[0].MatchType == protoobject.MatchType_STRING_EQUAL {
				// temporary add the only possible value just to calculate the cursor. Condition below reverts it.
				res[len(res)-1].Attributes = []string{body.Filters[0].Value}
			}
			if newCursor, err = objectcore.CalculateCursor(firstFilter, res[len(res)-1]); err != nil {
				return nil, nil, fmt.Errorf("recalculate cursor: %w", err)
			}
		}
	}

	if filteredAttributeless && (!localOnly || body.Filters[0].MatchType == protoobject.MatchType_STRING_EQUAL) {
		// for K=V queries, V is unambiguous, so there is no need to transmit it in each item
		for i := range res {
			res[i].Attributes = nil
		}
	}

	return res, newCursor, incomplete
}

func (s *Server) searchOnRemoteNode(ctx context.Context, node netmap.NodeInfo, req *protoobject.SearchV2Request) ([]client.SearchResultItem, bool, error) {
	c, err := s.nodeClients.Get(ctx, node)
	if err != nil {
		return nil, false, fmt.Errorf("get node client: %w", err)
	}

	var items []client.SearchResultItem
	var more bool
	return items, more, c.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		var err error
		items, more, err = searchOnRemoteAddress(ctx, conn, req)
		return err // TODO: log error
	})
}

func searchOnRemoteAddress(ctx context.Context, conn *grpc.ClientConn,
	req *protoobject.SearchV2Request) ([]client.SearchResultItem, bool, error) {
	resp, err := protoobject.NewObjectServiceClient(conn).SearchV2(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("send request over gRPC: %w", err)
	}

	if err := apistatus.ToError(resp.GetMetaHeader().GetStatus()); err != nil {
		return nil, false, err
	}
	// TODO: copy-pasted from SDK (*) client code, consider sharing
	//  (*) added check that cursor is set only when result has count items
	if resp.Body == nil {
		return nil, false, nil
	}
	n := uint32(len(resp.Body.Result))
	if n == 0 {
		if resp.Body.Cursor != "" {
			return nil, false, errors.New("invalid response body: cursor is set with empty result")
		}
		return nil, false, nil
	}
	if reqCursor := req.Body.Cursor; reqCursor != "" && resp.Body.Cursor == reqCursor {
		return nil, false, errors.New("invalid response body: cursor repeats the initial one")
	}
	if n > req.Body.Count {
		return nil, false, errors.New("invalid response body: more items than requested")
	}
	if resp.Body.Cursor != "" && n < req.Body.Count {
		return nil, false, fmt.Errorf("invalid response body: cursor is set with less items than requested %d < %d", n, req.Body.Count)
	}

	// TODO: we can theoretically do without type conversion, thus avoiding
	//  additional allocation. At the same time, this will require generic code for merging.
	res := make([]client.SearchResultItem, n)
	filteredAttributeless := len(req.Body.Attributes) == 0 && len(req.Body.Filters) > 0
	for i, r := range resp.Body.Result {
		switch {
		case r == nil:
			return nil, false, fmt.Errorf("invalid response body: nil element #%d", i)
		case r.Id == nil:
			return nil, false, fmt.Errorf("invalid response body: invalid element #%d: missing ID", i)
		case !filteredAttributeless && len(r.Attributes) != len(req.Body.Attributes) || filteredAttributeless && len(r.Attributes) > 1:
			return nil, false, fmt.Errorf("invalid response body: invalid element #%d: wrong attribute count %d", i, len(r.Attributes))
		}
		if err := res[i].ID.FromProtoMessage(r.Id); err != nil {
			return nil, false, fmt.Errorf("invalid response body: invalid element #%d: invalid ID: %w", i, err)
		}
		res[i].Attributes = r.Attributes
	}
	return res, resp.Body.Cursor != "", nil
}

func objectFromMessage(gMsg *protoobject.Object) (*object.Object, error) {
	var obj = new(object.Object)
	err := obj.FromProtoMessage(gMsg)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (s *Server) metaInfoSignature(o object.Object) ([]byte, error) {
	cnr, err := s.fsChain.Get(o.GetContainerID())
	if err != nil {
		return nil, fmt.Errorf("fetching container info: %w", err)
	}

	firstObj := o.GetFirstID()
	if o.HasParent() && firstObj.IsZero() {
		// object itself is the first one
		firstObj = o.GetID()
	}
	prevObj := o.GetPreviousID()

	var (
		deleted oid.ID
		locked  oid.ID
	)
	typ := o.Type()
	switch typ {
	case object.TypeTombstone:
		deleted = o.AssociatedObject()
	case object.TypeLock:
		locked = o.AssociatedObject()
	default:
	}

	currentBlock := s.meta.Height()
	currentEpochDuration := s.fsChain.CurrentEpochDuration()
	firstBlock := (uint64(currentBlock)/currentEpochDuration + 1) * currentEpochDuration
	secondBlock := firstBlock + currentEpochDuration
	thirdBlock := secondBlock + currentEpochDuration

	_, firstMeta := objectcore.EncodeChainMetaInfo(len(cnr.PlacementPolicy().Replicas()), o.GetContainerID(), o.GetID(), firstObj, prevObj, o.PayloadSize(), typ, deleted, locked, firstBlock, s.meta.MagicNumber())
	_, secondMeta := objectcore.EncodeChainMetaInfo(len(cnr.PlacementPolicy().Replicas()), o.GetContainerID(), o.GetID(), firstObj, prevObj, o.PayloadSize(), typ, deleted, locked, secondBlock, s.meta.MagicNumber())
	_, thirdMeta := objectcore.EncodeChainMetaInfo(len(cnr.PlacementPolicy().Replicas()), o.GetContainerID(), o.GetID(), firstObj, prevObj, o.PayloadSize(), typ, deleted, locked, thirdBlock, s.meta.MagicNumber())

	var firstSig neofscrypto.Signature
	var secondSig neofscrypto.Signature
	var thirdSig neofscrypto.Signature
	signer := neofsecdsa.SignerRFC6979(s.signer)
	err = firstSig.Calculate(signer, firstMeta)
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

	firstSigV2 := firstSig.ProtoMessage()
	secondSigV2 := secondSig.ProtoMessage()
	thirdSigV2 := thirdSig.ProtoMessage()

	res := make([]byte, 4+firstSigV2.MarshaledSize()+4+secondSigV2.MarshaledSize()+4+thirdSigV2.MarshaledSize())
	binary.LittleEndian.PutUint32(res, uint32(firstSigV2.MarshaledSize()))
	firstSigV2.MarshalStable(res[4 : 4+firstSigV2.MarshaledSize()])
	binary.LittleEndian.PutUint32(res[4+firstSigV2.MarshaledSize():], uint32(secondSigV2.MarshaledSize()))
	secondSigV2.MarshalStable(res[4+firstSigV2.MarshaledSize()+4 : 4+firstSigV2.MarshaledSize()+4+secondSigV2.MarshaledSize()])
	binary.LittleEndian.PutUint32(res[4+firstSigV2.MarshaledSize()+4+secondSigV2.MarshaledSize():], uint32(thirdSigV2.MarshaledSize()))
	thirdSigV2.MarshalStable(res[4+firstSigV2.MarshaledSize()+4+secondSigV2.MarshaledSize()+4:])

	return res, nil
}

func checkStatus(st *protostatus.Status) error {
	return apistatus.ToError(st)
}

func chunkBoundsToSend(global, local, chunkLen int) (int, int) {
	if global == local {
		return 0, chunkLen
	}
	if local+chunkLen <= global {
		// chunk has already been sent
		return 0, 0
	}
	return global - local, chunkLen
}

func needSignGetResponse(req util.Request) bool {
	return util.VersionLE(req, 2, 17)
}

func checkHeaderProtobufAgainstID(buffers iprotobuf.BuffersSlice, id oid.ID, ordered bool) error {
	b := buffers.ReadOnlyData()
	if !ordered {
		// TODO: consider optimization
		// Either require direct order in protocol (for example, current node does this) or use buffer from pool.
		var hdr protoobject.Header
		if err := proto.Unmarshal(buffers.ReadOnlyData(), &hdr); err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}
		b = make([]byte, hdr.MarshaledSize())
		hdr.MarshalStable(b)
	}

	return checkOrderedHeaderProtobufAgainstID(b, id)
}

func checkOrderedHeaderProtobufAgainstID(b []byte, id oid.ID) error {
	if oid.NewFromObjectHeaderBinary(b) != id {
		return errors.New("received header mismatches ID")
	}

	return nil
}
