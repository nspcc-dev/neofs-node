package object

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	metasvc "github.com/nspcc-dev/neofs-node/pkg/services/meta"
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	sdkclient "github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	sdknetmap "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/stat"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/panjf2000/ants/v2"
	"google.golang.org/grpc"
)

// Handlers represents storage node's internal handler Object service op
// payloads.
type Handlers interface {
	Get(context.Context, getsvc.Prm) error
	Put(context.Context) (*putsvc.Streamer, error)
	Head(context.Context, getsvc.HeadPrm) error
	Search(context.Context, searchsvc.Prm) error
	Delete(context.Context, deletesvc.Prm) error
	GetRange(context.Context, getsvc.RangePrm) error
	GetRangeHash(context.Context, getsvc.RangeHashPrm) (*getsvc.RangeHashRes, error)
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
	container.Source
	netmap.StateDetailed
	icrypto.N3ScriptRunner

	// ForEachContainerNodePublicKeyInLastTwoEpochs iterates over all nodes matching
	// the referenced container's storage policy at the current and the previous
	// NeoFS epochs, and passes their public keys into f. IterateContainerNodeKeys
	// breaks without an error when f returns false. Keys may be repeated.
	//
	// Returns [apistatus.ErrContainerNotFound] if referenced container was not
	// found.
	ForEachContainerNodePublicKeyInLastTwoEpochs(cid.ID, func(pubKey []byte) bool) error

	// ForEachContainerNode iterates over all nodes matching the referenced
	// container's storage policy for now and passes their descriptors into f.
	// IterateContainerNodeKeys breaks without an error when f returns false.
	// Elements may be repeated.
	//
	// Returns [apistatus.ErrContainerNotFound] if referenced container was not
	// found.
	ForEachContainerNode(cnr cid.ID, f func(sdknetmap.NodeInfo) bool) error

	// IsOwnPublicKey checks whether given pubKey assigned to Node in the NeoFS
	// network map.
	IsOwnPublicKey(pubKey []byte) bool

	// LocalNodeUnderMaintenance checks whether local node is under maintenance
	// according to the network map from FSChain.
	LocalNodeUnderMaintenance() bool
}

type sessions interface {
	// GetSessionPrivateKey reads private session key by user and session IDs.
	// Returns [apistatus.ErrSessionTokenNotFound] if there is no data for the
	// referenced session.
	GetSessionPrivateKey(usr user.ID, uid uuid.UUID) (ecdsa.PrivateKey, error)
}

// Storage groups ops of the node's storage required to serve NeoFS API Object
// service.
type Storage interface {
	sessions

	// VerifyAndStoreObjectLocally checks whether given object has correct format
	// and, if so, saves it in the Storage. StoreObject is called only when local
	// node complies with the container's storage policy.
	VerifyAndStoreObjectLocally(object.Object) error

	// SearchObjects selects up to count container's objects from the given
	// container matching the specified filters.
	SearchObjects(_ cid.ID, _ []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]sdkclient.SearchResultItem, []byte, error)
}

// ACLInfoExtractor is the interface that allows to fetch data required for ACL
// checks from various types of grpc requests.
type ACLInfoExtractor interface {
	PutRequestToInfo(*protoobject.PutRequest) (aclsvc.RequestInfo, user.ID, error)
	DeleteRequestToInfo(*protoobject.DeleteRequest) (aclsvc.RequestInfo, error)
	HeadRequestToInfo(*protoobject.HeadRequest) (aclsvc.RequestInfo, error)
	HashRequestToInfo(*protoobject.GetRangeHashRequest) (aclsvc.RequestInfo, error)
	GetRequestToInfo(*protoobject.GetRequest) (aclsvc.RequestInfo, error)
	RangeRequestToInfo(*protoobject.GetRangeRequest) (aclsvc.RequestInfo, error)
	SearchRequestToInfo(*protoobject.SearchRequest) (aclsvc.RequestInfo, error)
	SearchV2RequestToInfo(*protoobject.SearchV2Request) (aclsvc.RequestInfo, error)
}

const accessDeniedACLReasonFmt = "access to operation %s is denied by basic ACL check"
const accessDeniedEACLReasonFmt = "access to operation %s is denied by extended ACL check: %v"

func basicACLErr(info aclsvc.RequestInfo) error {
	var errAccessDenied apistatus.ObjectAccessDenied
	errAccessDenied.WriteReason(fmt.Sprintf(accessDeniedACLReasonFmt, info.Operation()))

	return errAccessDenied
}

func eACLErr(info aclsvc.RequestInfo, err error) error {
	var errAccessDenied apistatus.ObjectAccessDenied
	errAccessDenied.WriteReason(fmt.Sprintf(accessDeniedEACLReasonFmt, info.Operation(), err))

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
	mNumber       uint32
	metrics       MetricCollector
	aclChecker    aclsvc.ACLChecker
	reqInfoProc   ACLInfoExtractor
	nodeClients   searchsvc.ClientConstructor
	searchWorkers *ants.Pool
}

// New provides protoobject.ObjectServiceServer for the given parameters.
func New(hs Handlers, magicNumber uint32, fsChain FSChain, st Storage, metaSvc *metasvc.Meta, signer ecdsa.PrivateKey, m MetricCollector, ac aclsvc.ACLChecker, rp ACLInfoExtractor, cs searchsvc.ClientConstructor) *Server {
	// TODO: configurable capacity
	sp, err := ants.NewPool(100, ants.WithNonblocking(true))
	if err != nil {
		panic(fmt.Errorf("create ants pool: %w", err)) // fails on invalid input only
	}
	return &Server{
		handlers:      hs,
		fsChain:       fsChain,
		storage:       st,
		meta:          metaSvc,
		signer:        signer,
		mNumber:       magicNumber,
		metrics:       m,
		aclChecker:    ac,
		reqInfoProc:   rp,
		nodeClients:   cs,
		searchWorkers: sp,
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

func (s *Server) sendPutResponse(stream protoobject.ObjectService_PutServer, resp *protoobject.PutResponse) error {
	resp.VerifyHeader = util.SignResponse(&s.signer, resp)
	return stream.SendAndClose(resp)
}

func (s *Server) sendStatusPutResponse(stream protoobject.ObjectService_PutServer, err error) error {
	return s.sendPutResponse(stream, &protoobject.PutResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
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

func (x *putStream) sendToRemoteNode(node client.NodeInfo, c client.MultiAddressClient) error {
	nodePub := node.PublicKey()
	return c.ForEachGRPCConn(x.ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		return putToRemoteNode(ctx, conn, nodePub, x.initReq, x.chunkReqs) // TODO: log error
	})
}

func putToRemoteNode(ctx context.Context, conn *grpc.ClientConn, nodePub []byte,
	initReq *protoobject.PutRequest, chunkReqs []*protoobject.PutRequest) error {
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

	if err := internal.VerifyResponseKeyV2(nodePub, resp); err != nil {
		return err
	}
	if err := neofscrypto.VerifyResponseWithBuffer(resp, nil); err != nil {
		return fmt.Errorf("response verification failed: %w", err)
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

func (x *putStream) forwardRequest(req *protoobject.PutRequest) error {
	switch v := req.GetBody().GetObjectPart().(type) {
	default:
		return fmt.Errorf("invalid object put stream part type %T", v)
	case *protoobject.PutRequest_Body_Init_:
		if v == nil || v.Init == nil { // TODO: seems like this is done several times, deduplicate
			return errors.New("nil oneof field with heading part")
		}

		cp, err := objutil.CommonPrmFromRequest(req)
		if err != nil {
			return err
		}

		mo := &protoobject.Object{
			ObjectId:  v.Init.ObjectId,
			Signature: v.Init.Signature,
			Header:    v.Init.Header,
		}
		var obj = new(object.Object)
		err = obj.FromProtoMessage(mo)
		if err != nil {
			return err
		}

		var p putsvc.PutInitPrm
		p.WithCommonPrm(cp)
		p.WithObject(obj)
		p.WithCopiesNumber(v.Init.CopiesNumber)
		p.WithRelay(x.sendToRemoteNode)
		if err = x.base.Init(&p); err != nil {
			return fmt.Errorf("could not init object put stream: %w", err)
		}

		if x.cacheReqs = v.Init.Signature != nil; !x.cacheReqs {
			return nil
		}

		x.expBytes = v.Init.Header.GetPayloadLength()
		if m := x.base.MaxObjectSize(); x.expBytes > m {
			return putsvc.ErrExceedingMaxSize
		}
		signed, err := x.resignRequest(req) // TODO: resign only when needed
		if err != nil {
			return err // TODO: add context
		}
		x.initReq = signed
	case *protoobject.PutRequest_Body_Chunk:
		c := v.Chunk
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
	}
	return nil
}

func (x *putStream) close() (*protoobject.PutResponse, error) {
	if x.cacheReqs && x.recvBytes != x.expBytes {
		return nil, putsvc.ErrWrongPayloadSize
	}

	resp, err := x.base.Close()
	if err != nil {
		return nil, fmt.Errorf("could not object put stream: %w", err)
	}

	id := resp.ObjectID()
	return &protoobject.PutResponse{
		Body: &protoobject.PutResponse_Body{
			ObjectId: id.ProtoMessage(),
		},
	}, nil
}

func (s *Server) Put(gStream protoobject.ObjectService_PutServer) error {
	t := time.Now()
	stream, err := s.handlers.Put(gStream.Context())

	defer func() { s.pushOpExecResult(stat.MethodObjectPut, err, t) }()
	if err != nil {
		return err
	}

	var req *protoobject.PutRequest
	var resp *protoobject.PutResponse

	ps := newIntermediatePutStream(s.signer, stream, gStream.Context())
	for {
		if req, err = gStream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				resp, err = ps.close()
				if err != nil {
					return s.sendStatusPutResponse(gStream, err)
				}

				err = s.sendPutResponse(gStream, resp)
				return err
			}
			return err
		}

		if c := req.GetBody().GetChunk(); c != nil {
			s.metrics.AddPutPayload(len(c))
		}

		if err = icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
			err = s.sendStatusPutResponse(gStream, err) // assign for defer
			return err
		}

		if s.fsChain.LocalNodeUnderMaintenance() {
			return s.sendStatusPutResponse(gStream, apistatus.ErrNodeUnderMaintenance)
		}

		if req.Body == nil {
			return errors.New("malformed request: empty body")
		}

		if reqInfo, objOwner, err := s.reqInfoProc.PutRequestToInfo(req); err != nil {
			if !errors.Is(err, aclsvc.ErrSkipRequest) {
				return s.sendStatusPutResponse(gStream, err)
			}
		} else {
			if !s.aclChecker.CheckBasicACL(reqInfo) || !s.aclChecker.StickyBitCheck(reqInfo, objOwner) {
				err = basicACLErr(reqInfo) // needed for defer
				return s.sendStatusPutResponse(gStream, err)
			}
			err = s.aclChecker.CheckEACL(req, reqInfo)
			if err != nil {
				err = eACLErr(reqInfo, err) // needed for defer
				return s.sendStatusPutResponse(gStream, err)
			}
		}

		if err = ps.forwardRequest(req); err != nil {
			err = s.sendStatusPutResponse(gStream, err) // assign for defer
			return err
		}
	}
}

func (s *Server) signDeleteResponse(resp *protoobject.DeleteResponse) *protoobject.DeleteResponse {
	resp.VerifyHeader = util.SignResponse(&s.signer, resp)
	return resp
}

func (s *Server) makeStatusDeleteResponse(err error) *protoobject.DeleteResponse {
	return s.signDeleteResponse(&protoobject.DeleteResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
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
		return s.makeStatusDeleteResponse(err), nil
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusDeleteResponse(apistatus.ErrNodeUnderMaintenance), nil
	}

	reqInfo, err := s.reqInfoProc.DeleteRequestToInfo(req)
	if err != nil {
		return s.makeStatusDeleteResponse(err), nil
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.makeStatusDeleteResponse(err), nil
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // needed for defer
		return s.makeStatusDeleteResponse(err), nil
	}

	ma := req.GetBody().GetAddress()
	if ma == nil {
		return s.makeStatusDeleteResponse(errors.New("missing object address")), nil
	}
	var addr oid.Address
	err = addr.FromProtoMessage(ma)
	if err != nil {
		return s.makeStatusDeleteResponse(fmt.Errorf("invalid object address: %w", err)), nil
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return s.makeStatusDeleteResponse(err), nil
	}

	var rb protoobject.DeleteResponse_Body

	var p deletesvc.Prm
	p.SetCommonParameters(cp)
	p.WithAddress(addr)
	p.WithTombstoneAddressTarget((*deleteResponseBody)(&rb))
	err = s.handlers.Delete(ctx, p)
	if err != nil {
		return s.makeStatusDeleteResponse(err), nil
	}

	return s.signDeleteResponse(&protoobject.DeleteResponse{Body: &rb}), nil
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

func (s *Server) Head(ctx context.Context, req *protoobject.HeadRequest) (*protoobject.HeadResponse, error) {
	var (
		err error
		t   = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectHead, err, t) }()

	needSignResp := needSignGetResponse(req)

	if err := icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
		return s.makeStatusHeadResponse(err, needSignResp), nil
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusHeadResponse(apistatus.ErrNodeUnderMaintenance, needSignResp), nil
	}

	reqInfo, err := s.reqInfoProc.HeadRequestToInfo(req)
	if err != nil {
		return s.makeStatusHeadResponse(err, needSignResp), nil
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.makeStatusHeadResponse(err, needSignResp), nil
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // needed for defer
		return s.makeStatusHeadResponse(err, needSignResp), nil
	}

	var resp protoobject.HeadResponse
	p, err := convertHeadPrm(s.signer, req, &resp)
	if err != nil {
		return s.makeStatusHeadResponse(err, needSignResp), nil
	}
	err = s.handlers.Head(ctx, p)
	if err != nil {
		return s.makeStatusHeadResponse(err, needSignResp), nil
	}

	err = s.aclChecker.CheckEACL(&resp, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // defer
		return s.makeStatusHeadResponse(err, needSignResp), nil
	}

	return s.signHeadResponse(&resp, needSignResp), nil
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
func convertHeadPrm(signer ecdsa.PrivateKey, req *protoobject.HeadRequest, resp *protoobject.HeadResponse) (getsvc.HeadPrm, error) {
	body := req.GetBody()
	ma := body.GetAddress()
	if ma == nil { // includes nil body
		return getsvc.HeadPrm{}, errors.New("missing object address")
	}

	var addr oid.Address
	if err := addr.FromProtoMessage(ma); err != nil {
		return getsvc.HeadPrm{}, fmt.Errorf("invalid object address: %w", err)
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return getsvc.HeadPrm{}, err
	}

	var p getsvc.HeadPrm
	p.SetCommonParameters(cp)
	p.WithAddress(addr)
	p.WithRawFlag(body.Raw)
	p.SetHeaderWriter(&headResponse{
		dst: resp,
	})
	if cp.LocalOnly() {
		return p, nil
	}

	var onceResign sync.Once
	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.HeadPrm{}, errors.New("missing meta header")
	}
	p.SetRequestForwarder(func(ctx context.Context, node client.NodeInfo, c client.MultiAddressClient) (*object.Object, error) {
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
			return nil, err
		}

		var hdr *object.Object
		return hdr, c.ForEachGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			var err error
			hdr, err = getHeaderFromRemoteNode(ctx, conn, req, addr.Object())
			return err // TODO: log error
		})
	})
	return p, nil
}

func getHeaderFromRemoteNode(ctx context.Context, conn *grpc.ClientConn, req *protoobject.HeadRequest, reqOID oid.ID) (*object.Object, error) {
	resp, err := protoobject.NewObjectServiceClient(conn).Head(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending the request failed: %w", err)
	}

	if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
		return nil, err
	}

	var hdr *protoobject.Header
	var idSig *refs.Signature
	switch v := resp.GetBody().GetHead().(type) {
	case nil:
		return nil, fmt.Errorf("unexpected header type %T", v)
	case *protoobject.HeadResponse_Body_ShortHeader:
		return nil, fmt.Errorf("unsupported short header")
	case *protoobject.HeadResponse_Body_Header:
		if v == nil || v.Header == nil {
			return nil, errors.New("nil header oneof field")
		}
		if v.Header.Header == nil {
			return nil, errors.New("missing header")
		}
		if v.Header.Signature == nil {
			// TODO(@cthulhu-rider): #1387 use "const" error
			return nil, errors.New("missing signature")
		}

		if err := checkHeaderAgainstID(v.Header.Header, reqOID); err != nil {
			return nil, err
		}

		hdr = v.Header.Header
		idSig = v.Header.Signature
	case *protoobject.HeadResponse_Body_SplitInfo:
		if v == nil || v.SplitInfo == nil {
			return nil, errors.New("nil split info oneof field")
		}
		si := object.NewSplitInfo()
		err := si.FromProtoMessage(v.SplitInfo)
		if err != nil {
			return nil, err
		}
		return nil, object.NewSplitInfoError(si)
	}

	mObj := &protoobject.Object{
		Signature: idSig,
		Header:    hdr,
	}
	var obj = object.New()
	if err := obj.FromProtoMessage(mObj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Server) signHashResponse(resp *protoobject.GetRangeHashResponse) *protoobject.GetRangeHashResponse {
	resp.VerifyHeader = util.SignResponse(&s.signer, resp)
	return resp
}

func (s *Server) makeStatusHashResponse(err error) *protoobject.GetRangeHashResponse {
	return s.signHashResponse(&protoobject.GetRangeHashResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

// GetRangeHash converts gRPC GetRangeHashRequest message and passes it to internal Object service.
func (s *Server) GetRangeHash(ctx context.Context, req *protoobject.GetRangeHashRequest) (*protoobject.GetRangeHashResponse, error) {
	var (
		err error
		t   = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectHash, err, t) }()
	if err = icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
		return s.makeStatusHashResponse(err), nil
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusHashResponse(apistatus.ErrNodeUnderMaintenance), nil
	}

	reqInfo, err := s.reqInfoProc.HashRequestToInfo(req)
	if err != nil {
		return s.makeStatusHashResponse(err), nil
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.makeStatusHashResponse(err), nil
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // needed for defer
		return s.makeStatusHashResponse(err), nil
	}

	p, err := convertHashPrm(s.signer, s.storage, req)
	if err != nil {
		return s.makeStatusHashResponse(err), nil
	}
	res, err := s.handlers.GetRangeHash(ctx, p)
	if err != nil {
		return s.makeStatusHashResponse(err), nil
	}

	return s.signHashResponse(&protoobject.GetRangeHashResponse{
		Body: &protoobject.GetRangeHashResponse_Body{
			Type:     req.Body.Type,
			HashList: res.Hashes(),
		}}), nil
}

// converts original request into parameters accepted by the internal handler.
func convertHashPrm(signer ecdsa.PrivateKey, ss sessions, req *protoobject.GetRangeHashRequest) (getsvc.RangeHashPrm, error) {
	body := req.GetBody()
	ma := body.GetAddress()
	if ma == nil { // includes nil body
		return getsvc.RangeHashPrm{}, errors.New("missing object address")
	}

	var addr oid.Address
	if err := addr.FromProtoMessage(ma); err != nil {
		return getsvc.RangeHashPrm{}, fmt.Errorf("invalid object address: %w", err)
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return getsvc.RangeHashPrm{}, err
	}

	var p getsvc.RangeHashPrm

	switch t := body.GetType(); t {
	default:
		return getsvc.RangeHashPrm{}, fmt.Errorf("unknown checksum type %v", t)
	case refs.ChecksumType_SHA256:
		p.SetHashGenerator(sha256.New)
	case refs.ChecksumType_TZ:
		p.SetHashGenerator(tz.New)
	}

	if tok := cp.SessionToken(); tok != nil {
		signerKey, err := ss.GetSessionPrivateKey(tok.Issuer(), tok.ID())
		if err != nil {
			if !errors.Is(err, apistatus.ErrSessionTokenNotFound) {
				return getsvc.RangeHashPrm{}, fmt.Errorf("fetching session key: %w", err)
			}
			cp.ForgetTokens()
			signerKey = signer
		}
		p.WithCachedSignerKey(&signerKey)
	}

	mr := body.GetRanges()
	rngs := make([]object.Range, len(mr))
	for i := range mr {
		rngs[i].SetOffset(mr[i].Offset)
		rngs[i].SetLength(mr[i].Length)
	}

	p.SetCommonParameters(cp)
	p.WithAddress(addr)
	p.SetRangeList(rngs)
	p.SetSalt(body.GetSalt())

	if cp.LocalOnly() {
		return p, nil
	}

	var onceResign sync.Once
	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.RangeHashPrm{}, errors.New("missing meta header")
	}
	p.SetRangeHashRequestForwarder(func(ctx context.Context, node client.NodeInfo, c client.MultiAddressClient) ([][]byte, error) {
		var err error
		onceResign.Do(func() {
			req.MetaHeader = &protosession.RequestMetaHeader{
				// TODO: #1165 think how to set the other fields
				Version: newCurrentProtoVersionMessage(),
				Ttl:     meta.GetTtl() - 1,
				Origin:  meta,
			}
			req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(neofsecdsa.Signer(signer), req, nil)
		})
		if err != nil {
			return nil, err
		}

		nodePub := node.PublicKey()
		var hs [][]byte
		return hs, c.ForEachGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			var err error
			hs, err = getHashesFromRemoteNode(ctx, conn, nodePub, req)
			return err // TODO: log error
		})
	})
	return p, nil
}

func getHashesFromRemoteNode(ctx context.Context, conn *grpc.ClientConn, nodePub []byte,
	req *protoobject.GetRangeHashRequest) ([][]byte, error) {
	resp, err := protoobject.NewObjectServiceClient(conn).GetRangeHash(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetRangeHash rpc failure: %w", err)
	}

	if err := internal.VerifyResponseKeyV2(nodePub, resp); err != nil {
		return nil, err
	}
	if err := neofscrypto.VerifyResponseWithBuffer(resp, nil); err != nil {
		return nil, fmt.Errorf("response verification failed: %w", err)
	}
	if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
		return nil, err
	}
	// TODO: verify number of hashes
	return resp.GetBody().GetHashList(), nil
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
	reqInfo aclsvc.RequestInfo

	signResponse bool
}

func (s *getStream) WriteHeader(hdr *object.Object) error {
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
	if err := s.srv.aclChecker.CheckEACL(resp, s.reqInfo); err != nil {
		return eACLErr(s.reqInfo, err)
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
		err error
		t   = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectGet, err, t) }()

	needSignResp := needSignGetResponse(req)

	if err = icrypto.VerifyRequestSignatures(req); err != nil {
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusGetResponse(gStream, apistatus.ErrNodeUnderMaintenance, needSignResp)
	}

	reqInfo, err := s.reqInfoProc.GetRequestToInfo(req)
	if err != nil {
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // needed for defer
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}

	p, err := convertGetPrm(s.signer, req, &getStream{
		base:         gStream,
		srv:          s,
		reqInfo:      reqInfo,
		signResponse: needSignResp,
	})
	if err != nil {
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}
	err = s.handlers.Get(gStream.Context(), p)
	if err != nil {
		return s.sendStatusGetResponse(gStream, err, needSignResp)
	}
	return nil
}

// converts original request into parameters accepted by the internal handler.
// Note that the stream is untouched within this call, errors are not reported
// into it.
func convertGetPrm(signer ecdsa.PrivateKey, req *protoobject.GetRequest, stream *getStream) (getsvc.Prm, error) {
	body := req.GetBody()
	ma := body.GetAddress()
	if ma == nil { // includes nil body
		return getsvc.Prm{}, errors.New("missing object address")
	}

	var addr oid.Address
	if err := addr.FromProtoMessage(ma); err != nil {
		return getsvc.Prm{}, fmt.Errorf("invalid object address: %w", err)
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return getsvc.Prm{}, err
	}

	var p getsvc.Prm
	p.SetCommonParameters(cp)
	p.WithAddress(addr)
	p.WithRawFlag(body.Raw)
	p.SetObjectWriter(stream)
	if cp.LocalOnly() {
		return p, nil
	}

	var onceResign sync.Once
	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.Prm{}, errors.New("missing meta header")
	}

	proxyCtx := getProxyContext{
		req:        req,
		reqOID:     addr.Object(),
		respStream: stream,
	}

	p.SetRequestForwarder(func(ctx context.Context, node client.NodeInfo, c client.MultiAddressClient) (*object.Object, error) {
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
			return nil, err
		}

		return nil, c.ForEachGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			err := proxyCtx.continueWithConn(ctx, conn)
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err // TODO: log error
		})
	})
	return p, nil
}

type getProxyContext struct {
	req        *protoobject.GetRequest
	reqOID     oid.ID
	respStream *getStream

	onceHdr sync.Once

	payloadLenCheck  uint64
	payloadHashCheck []byte

	respondedPayload int
	payloadHashGot   hash.Hash
}

func (x *getProxyContext) continueWithConn(ctx context.Context, conn *grpc.ClientConn) error {
	getStream, err := protoobject.NewObjectServiceClient(conn).Get(ctx, x.req)
	if err != nil {
		return fmt.Errorf("stream opening failed: %w", err)
	}

	var headWas bool
	var readPayload int
	for {
		resp, err := getStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if !headWas {
					return io.ErrUnexpectedEOF
				}
				return io.EOF
			}
			return fmt.Errorf("reading the response failed: %w", err)
		}

		if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
			return err
		}

		switch v := resp.GetBody().GetObjectPart().(type) {
		default:
			return fmt.Errorf("unexpected object part %T", v)
		case *protoobject.GetResponse_Body_Init_:
			if headWas {
				return errors.New("incorrect message sequence")
			}
			headWas = true
			if v == nil || v.Init == nil {
				return errors.New("nil header oneof field")
			}

			if v.Init.Header == nil {
				return errors.New("invalid response: missing header")
			}
			if v.Init.Header.PayloadHash == nil {
				return errors.New("invalid response: invalid header: missing payload hash")
			}
			if err := checkHeaderAgainstID(v.Init.Header, x.reqOID); err != nil {
				return err
			}

			mo := &protoobject.Object{
				ObjectId:  v.Init.ObjectId,
				Signature: v.Init.Signature,
				Header:    v.Init.Header,
			}
			obj := object.New()
			err := obj.FromProtoMessage(mo)
			if err != nil {
				return err
			}
			x.onceHdr.Do(func() {
				err = x.respStream.WriteHeader(obj)
			})
			if err != nil {
				return fmt.Errorf("could not write object header in Get forwarder: %w", err)
			}

			x.payloadLenCheck = v.Init.Header.PayloadLength
			x.payloadHashCheck = v.Init.Header.PayloadHash.Sum
			x.payloadHashGot = sha256.New()
		case *protoobject.GetResponse_Body_Chunk:
			if !headWas {
				return errors.New("incorrect message sequence")
			}
			fullChunk := v.Chunk
			respChunk := chunkToSend(x.respondedPayload, readPayload, fullChunk)
			if len(respChunk) == 0 {
				readPayload += len(fullChunk)
				continue
			}

			x.payloadHashGot.Write(respChunk) // never returns an error according to docs

			if uint64(x.respondedPayload+len(respChunk)) == x.payloadLenCheck {
				if !bytes.Equal(x.payloadHashGot.Sum(nil), x.payloadHashCheck) { // not merged via && for readability
					return errors.New("received payload mismatches checksum from header")
				}
			}

			if err := x.respStream.WriteChunk(respChunk); err != nil {
				return fmt.Errorf("could not write object chunk in Get forwarder: %w", err)
			}
			readPayload += len(fullChunk)
			x.respondedPayload += len(respChunk)
		case *protoobject.GetResponse_Body_SplitInfo:
			if v == nil || v.SplitInfo == nil {
				return errors.New("nil split info oneof field")
			}
			si := object.NewSplitInfo()
			err := si.FromProtoMessage(v.SplitInfo)
			if err != nil {
				return err
			}
			return object.NewSplitInfoError(si)
		}
	}
}

func (s *Server) sendRangeResponse(stream protoobject.ObjectService_GetRangeServer, resp *protoobject.GetRangeResponse) error {
	resp.VerifyHeader = util.SignResponse(&s.signer, resp)
	return stream.Send(resp)
}

func (s *Server) sendStatusRangeResponse(stream protoobject.ObjectService_GetRangeServer, err error) error {
	var splitErr *object.SplitInfoError
	if errors.As(err, &splitErr) {
		return s.sendRangeResponse(stream, &protoobject.GetRangeResponse{
			Body: &protoobject.GetRangeResponse_Body{
				RangePart: &protoobject.GetRangeResponse_Body_SplitInfo{
					SplitInfo: splitErr.SplitInfo().ProtoMessage(),
				},
			},
		})
	}
	return s.sendRangeResponse(stream, &protoobject.GetRangeResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

type rangeStream struct {
	base    protoobject.ObjectService_GetRangeServer
	srv     *Server
	reqInfo aclsvc.RequestInfo
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
		// TODO: do not check response multiple times
		// TODO: why check it at all?
		if err := s.srv.aclChecker.CheckEACL(newResp, s.reqInfo); err != nil {
			return eACLErr(s.reqInfo, err)
		}
		if err := s.srv.sendRangeResponse(s.base, newResp); err != nil {
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
		return s.sendStatusRangeResponse(gStream, err)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusRangeResponse(gStream, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.RangeRequestToInfo(req)
	if err != nil {
		return s.sendStatusRangeResponse(gStream, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.sendStatusRangeResponse(gStream, err)
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // needed for defer
		return s.sendStatusRangeResponse(gStream, err)
	}

	p, err := convertRangePrm(s.signer, req, &rangeStream{
		base:    gStream,
		srv:     s,
		reqInfo: reqInfo,
	})
	if err != nil {
		return s.sendStatusRangeResponse(gStream, err)
	}
	err = s.handlers.GetRange(gStream.Context(), p)
	if err != nil {
		return s.sendStatusRangeResponse(gStream, err)
	}
	return nil
}

// converts original request into parameters accepted by the internal handler.
// Note that the stream is untouched within this call, errors are not reported
// into it.
func convertRangePrm(signer ecdsa.PrivateKey, req *protoobject.GetRangeRequest, stream *rangeStream) (getsvc.RangePrm, error) {
	body := req.GetBody()
	ma := body.GetAddress()
	if ma == nil { // includes nil body
		return getsvc.RangePrm{}, errors.New("missing object address")
	}

	var addr oid.Address
	if err := addr.FromProtoMessage(ma); err != nil {
		return getsvc.RangePrm{}, fmt.Errorf("invalid object address: %w", err)
	}

	rln := body.Range.GetLength()
	if rln == 0 { // includes nil range
		if body.Range.Offset != 0 {
			return getsvc.RangePrm{}, errors.New("zero range length")
		} // else whole payload
	} else if body.Range.Offset+rln <= body.Range.Offset {
		return getsvc.RangePrm{}, errors.New("range overflow")
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return getsvc.RangePrm{}, err
	}

	var p getsvc.RangePrm
	p.SetCommonParameters(cp)
	p.WithAddress(addr)
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
	var respondedPayload int
	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.RangePrm{}, errors.New("missing meta header")
	}
	p.SetRequestForwarder(func(ctx context.Context, node client.NodeInfo, c client.MultiAddressClient) (*object.Object, error) {
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
			return nil, err
		}

		nodePub := node.PublicKey()
		return nil, c.ForEachGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			err := continueRangeFromRemoteNode(ctx, conn, nodePub, req, stream, &respondedPayload)
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err // TODO: log error
		})
	})
	return p, nil
}

func continueRangeFromRemoteNode(ctx context.Context, conn *grpc.ClientConn, nodePub []byte, req *protoobject.GetRangeRequest,
	stream *rangeStream, respondedPayload *int) error {
	rangeStream, err := protoobject.NewObjectServiceClient(conn).GetRange(ctx, req)
	if err != nil {
		return fmt.Errorf("stream opening failed: %w", err)
	}

	var readPayload int
	for {
		resp, err := rangeStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			return fmt.Errorf("reading the response failed: %w", err)
		}

		if err = internal.VerifyResponseKeyV2(nodePub, resp); err != nil {
			return err
		}
		if err := neofscrypto.VerifyResponseWithBuffer(resp, nil); err != nil {
			return fmt.Errorf("response verification failed: %w", err)
		}
		if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
			return err
		}

		switch v := resp.GetBody().GetRangePart().(type) {
		default:
			return fmt.Errorf("unexpected range type %T", v)
		case *protoobject.GetRangeResponse_Body_Chunk:
			fullChunk := v.Chunk
			respChunk := chunkToSend(*respondedPayload, readPayload, fullChunk)
			if len(respChunk) == 0 {
				readPayload += len(fullChunk)
				continue
			}
			if err := stream.WriteChunk(respChunk); err != nil {
				return fmt.Errorf("could not write object chunk in Get forwarder: %w", err)
			}
			readPayload += len(fullChunk)
			*respondedPayload += len(respChunk)
		case *protoobject.GetRangeResponse_Body_SplitInfo:
			if v == nil || v.SplitInfo == nil {
				return errors.New("nil split info oneof field")
			}
			si := object.NewSplitInfo()
			err := si.FromProtoMessage(v.SplitInfo)
			if err != nil {
				return err
			}
			return object.NewSplitInfoError(si)
		}
	}
}

func (s *Server) sendSearchResponse(stream protoobject.ObjectService_SearchServer, resp *protoobject.SearchResponse) error {
	resp.VerifyHeader = util.SignResponse(&s.signer, resp)
	return stream.Send(resp)
}

func (s *Server) sendStatusSearchResponse(stream protoobject.ObjectService_SearchServer, err error) error {
	return s.sendSearchResponse(stream, &protoobject.SearchResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

type searchStream struct {
	base    protoobject.ObjectService_SearchServer
	srv     *Server
	reqInfo aclsvc.RequestInfo
}

func (s *searchStream) WriteIDs(ids []oid.ID) error {
	for len(ids) > 0 {
		// gRPC can retain message for some time, this is why we create full new message each time
		r := &protoobject.SearchResponse{
			Body: &protoobject.SearchResponse_Body{},
		}

		cut := min(maxObjAddrRespAmount, len(ids))

		r.Body.IdList = make([]*refs.ObjectID, cut)
		for i := range cut {
			r.Body.IdList[i] = ids[i].ProtoMessage()
		}
		// TODO: do not check response multiple times
		// TODO: why check it at all?
		if err := s.srv.aclChecker.CheckEACL(r, s.reqInfo); err != nil {
			return eACLErr(s.reqInfo, err)
		}
		if err := s.srv.sendSearchResponse(s.base, r); err != nil {
			return err
		}

		ids = ids[cut:]
	}
	return nil
}

func (s *Server) Search(req *protoobject.SearchRequest, gStream protoobject.ObjectService_SearchServer) error {
	var (
		err error
		t   = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectSearch, err, t) }()
	if err = icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
		return s.sendStatusSearchResponse(gStream, err)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusSearchResponse(gStream, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.SearchRequestToInfo(req)
	if err != nil {
		return s.sendStatusSearchResponse(gStream, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.sendStatusSearchResponse(gStream, err)
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err)
		return s.sendStatusSearchResponse(gStream, err)
	}

	p, err := convertSearchPrm(gStream.Context(), s.signer, req, &searchStream{
		base:    gStream,
		srv:     s,
		reqInfo: reqInfo,
	})
	if err != nil {
		return s.sendStatusSearchResponse(gStream, err)
	}
	err = s.handlers.Search(gStream.Context(), p)
	if err != nil {
		return s.sendStatusSearchResponse(gStream, err)
	}
	return nil
}

// converts original request into parameters accepted by the internal handler.
// Note that the stream is untouched within this call, errors are not reported
// into it.
func convertSearchPrm(ctx context.Context, signer ecdsa.PrivateKey, req *protoobject.SearchRequest, stream *searchStream) (searchsvc.Prm, error) {
	body := req.GetBody()
	mc := body.GetContainerId()
	if mc == nil {
		return searchsvc.Prm{}, errors.New("missing container ID")
	}

	var id cid.ID
	if err := id.FromProtoMessage(mc); err != nil {
		return searchsvc.Prm{}, fmt.Errorf("invalid container ID: %w", err)
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return searchsvc.Prm{}, err
	}

	mfs := body.GetFilters()
	ofs := object.NewSearchFilters()
	err = ofs.FromProtoMessage(mfs)
	if err != nil {
		return searchsvc.Prm{}, err
	}

	var p searchsvc.Prm
	p.SetCommonParameters(cp)
	p.WithContainerID(id)
	p.WithSearchFilters(ofs)
	p.SetWriter(stream)
	if cp.LocalOnly() {
		return p, nil
	}

	var onceResign sync.Once
	meta := req.GetMetaHeader()
	if meta == nil {
		return searchsvc.Prm{}, errors.New("missing meta header")
	}
	p.SetRequestForwarder(func(node client.NodeInfo, c client.MultiAddressClient) ([]oid.ID, error) {
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
			return nil, err
		}

		nodePub := node.PublicKey()
		var res []oid.ID
		return res, c.ForEachGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			var err error
			res, err = searchOnRemoteNode(ctx, conn, nodePub, req)
			return err // TODO: log error
		})
	})
	return p, nil
}

func searchOnRemoteNode(ctx context.Context, conn *grpc.ClientConn, nodePub []byte, req *protoobject.SearchRequest) ([]oid.ID, error) {
	searchStream, err := protoobject.NewObjectServiceClient(conn).Search(ctx, req)
	if err != nil {
		return nil, err
	}

	var res []oid.ID
	for {
		resp, err := searchStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return res, nil
			}
			return nil, fmt.Errorf("reading the response failed: %w", err)
		}

		if err := internal.VerifyResponseKeyV2(nodePub, resp); err != nil {
			return nil, err
		}
		if err := neofscrypto.VerifyResponseWithBuffer(resp, nil); err != nil {
			return nil, fmt.Errorf("could not verify %T: %w", resp, err)
		}
		if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
			return nil, fmt.Errorf("remote node response: %w", err)
		}

		chunk := resp.GetBody().GetIdList()
		var id oid.ID
		for i := range chunk {
			if err := id.FromProtoMessage(chunk[i]); err != nil {
				return nil, fmt.Errorf("invalid object ID: %w", err)
			}
			res = append(res, id)
		}
	}
}

// Replicate serves neo.fs.v2.object.ObjectService/Replicate RPC.
func (s *Server) Replicate(_ context.Context, req *protoobject.ReplicateRequest) (*protoobject.ReplicateResponse, error) {
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
	err := cnr.FromProtoMessage(gCnrMsg)
	if err != nil {
		return &protoobject.ReplicateResponse{Status: &protostatus.Status{
			Code:    codeInternal,
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

	err = s.storage.VerifyAndStoreObjectLocally(*obj)
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

func (s *Server) signSearchResponse(resp *protoobject.SearchV2Response) *protoobject.SearchV2Response {
	resp.VerifyHeader = util.SignResponse(&s.signer, resp)
	return resp
}

func (s *Server) makeStatusSearchResponse(err error) *protoobject.SearchV2Response {
	return s.signSearchResponse(&protoobject.SearchV2Response{
		MetaHeader: s.makeResponseMetaHeader(apistatus.FromError(err)),
	})
}

func (s *Server) SearchV2(ctx context.Context, req *protoobject.SearchV2Request) (*protoobject.SearchV2Response, error) {
	var (
		err error
		t   = time.Now()
	)
	defer s.pushOpExecResult(stat.MethodObjectSearchV2, err, t)
	if err = icrypto.VerifyRequestSignaturesN3(req, s.fsChain); err != nil {
		return s.makeStatusSearchResponse(err), nil
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusSearchResponse(apistatus.ErrNodeUnderMaintenance), nil
	}

	reqInfo, err := s.reqInfoProc.SearchV2RequestToInfo(req)
	if err != nil {
		return s.makeStatusSearchResponse(err), nil
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.makeStatusSearchResponse(err), nil
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err)
		return s.makeStatusSearchResponse(err), nil
	}

	body, err := s.processSearchRequest(ctx, req)
	if err != nil {
		return s.makeStatusSearchResponse(err), nil
	}
	return s.signSearchResponse(&protoobject.SearchV2Response{Body: body}), nil
}

func verifySearchFilter(f *protoobject.SearchFilter) error {
	switch f.Key {
	case "":
		return errors.New("missing attribute")
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

func (s *Server) processSearchRequest(ctx context.Context, req *protoobject.SearchV2Request) (*protoobject.SearchV2Response_Body, error) {
	body := req.GetBody()
	if body == nil {
		return nil, errors.New("missing body")
	}
	if body.ContainerId == nil {
		return nil, errors.New("missing container ID")
	}
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

	res, newCursor, err := s.ProcessSearch(ctx, req)
	if err != nil {
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
	return resBody, nil
}

func (s *Server) ProcessSearch(ctx context.Context, req *protoobject.SearchV2Request) ([]sdkclient.SearchResultItem, []byte, error) {
	ttl := req.MetaHeader.GetTtl()
	if ttl == 0 {
		return nil, nil, errors.New("zero TTL")
	}

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
		return nil, nil, err
	}

	var cID cid.ID
	if err = cID.FromProtoMessage(body.ContainerId); err != nil {
		return nil, nil, fmt.Errorf("invalid container ID: %w", err)
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

	var res []sdkclient.SearchResultItem
	var newCursor []byte
	count := uint16(body.Count) // legit according to the limit
	switch {
	case ttl == 1:
		if res, newCursor, err = s.storage.SearchObjects(cID, ofs, attrs, cursor, count); err != nil {
			return nil, nil, err
		}
	case handleWithMetaService:
		res, newCursor, err = s.meta.Search(cID, ofs, attrs, cursor, count)
		if err != nil {
			return nil, nil, err
		}
	default:
		var signed bool
		var resErr error
		mProcessedNodes := make(map[string]struct{})
		var sets [][]sdkclient.SearchResultItem
		var mores []bool
		var mtx sync.Mutex
		var wg sync.WaitGroup
		add := func(set []sdkclient.SearchResultItem, more bool) {
			mtx.Lock()
			sets, mores = append(sets, set), append(mores, more)
			mtx.Unlock()
		}
		err = s.fsChain.ForEachContainerNode(cID, func(node sdknetmap.NodeInfo) bool {
			nodePub := node.PublicKey()
			strKey := string(nodePub)
			if _, ok := mProcessedNodes[strKey]; ok {
				return true
			}
			mProcessedNodes[strKey] = struct{}{}
			if s.fsChain.IsOwnPublicKey(nodePub) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if set, crsr, err := s.storage.SearchObjects(cID, ofs, attrs, cursor, count); err == nil {
						add(set, crsr != nil)
					} // TODO: else log error
				}()
				return true
			}
			if !signed {
				req.MetaHeader = &protosession.RequestMetaHeader{Ttl: 1, Origin: req.MetaHeader}
				if req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer[*protoobject.SearchV2Request_Body](neofsecdsa.Signer(s.signer), req, nil); err != nil {
					resErr = fmt.Errorf("sign request: %w", err)
					return false
				}
				signed = true
			}
			wg.Add(1)
			if resErr = s.searchWorkers.Submit(func() {
				defer wg.Done()
				if set, more, err := s.searchOnRemoteNode(ctx, node, req); err == nil {
					add(set, more)
				} // TODO: else log error
			}); resErr != nil {
				wg.Done()
			}
			return resErr == nil
		})
		wg.Wait()
		if err == nil {
			err = resErr
		}
		if err != nil {
			return nil, nil, err
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

	if filteredAttributeless && (ttl != 1 || body.Filters[0].MatchType == protoobject.MatchType_STRING_EQUAL) {
		// for K=V queries, V is unambiguous, so there is no need to transmit it in each item
		for i := range res {
			res[i].Attributes = nil
		}
	}

	return res, newCursor, nil
}

func (s *Server) searchOnRemoteNode(ctx context.Context, node sdknetmap.NodeInfo, req *protoobject.SearchV2Request) ([]sdkclient.SearchResultItem, bool, error) {
	// TODO: copy-pasted from old search implementation, consider deduplicating in
	//  the client constructor
	var endpoints network.AddressGroup
	if err := endpoints.FromIterator(network.NodeEndpointsIterator(node)); err != nil {
		// critical error that may ultimately block the storage service. Normally it
		// should not appear because entry into the network map under strict control.
		return nil, false, fmt.Errorf("failed to decode network endpoints of the storage node from the network map: %w", err)
	}
	var info client.NodeInfo
	info.SetAddressGroup(endpoints)
	nodePub := node.PublicKey()
	info.SetPublicKey(nodePub)
	c, err := s.nodeClients.Get(info)
	if err != nil {
		return nil, false, fmt.Errorf("get node client: %w", err)
	}

	var items []sdkclient.SearchResultItem
	var more bool
	return items, more, c.ForEachGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		var err error
		items, more, err = searchOnRemoteAddress(ctx, conn, nodePub, req)
		return err // TODO: log error
	})
}

func searchOnRemoteAddress(ctx context.Context, conn *grpc.ClientConn, nodePub []byte,
	req *protoobject.SearchV2Request) ([]sdkclient.SearchResultItem, bool, error) {
	resp, err := protoobject.NewObjectServiceClient(conn).SearchV2(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("send request over gRPC: %w", err)
	}

	if !bytes.Equal(resp.GetVerifyHeader().GetBodySignature().GetKey(), nodePub) {
		return nil, false, client.ErrWrongPublicKey
	}
	if err := neofscrypto.VerifyResponseWithBuffer(resp, nil); err != nil {
		return nil, false, fmt.Errorf("response verification failed: %w", err)
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
	res := make([]sdkclient.SearchResultItem, n)
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
	var obj = object.New()
	err := obj.FromProtoMessage(gMsg)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (s *Server) metaInfoSignature(o object.Object) ([]byte, error) {
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

func chunkToSend(global, local int, chunk []byte) []byte {
	if global == local {
		return chunk
	}
	if local+len(chunk) <= global {
		// chunk has already been sent
		return nil
	}
	return chunk[global-local:]
}

func needSignGetResponse(req interface {
	GetMetaHeader() *protosession.RequestMetaHeader
}) bool {
	ver := req.GetMetaHeader().GetVersion()
	mjr := ver.GetMajor() // NPE-safe
	return mjr < 2 || mjr == 2 && ver.GetMinor() <= 17
}

func checkHeaderAgainstID(hdr *protoobject.Header, id oid.ID) error {
	b := make([]byte, hdr.MarshaledSize())
	hdr.MarshalStable(b)

	if oid.NewFromObjectHeaderBinary(b) != id {
		return errors.New("received header mismatches ID")
	}

	return nil
}
