package object

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	protoobject "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	refsv2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	protosession "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/status"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/stat"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
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

type server struct {
	handlers    Handlers
	fsChain     FSChain
	storage     Storage
	signer      ecdsa.PrivateKey
	mNumber     uint32
	metrics     MetricCollector
	aclChecker  aclsvc.ACLChecker
	reqInfoProc ACLInfoExtractor
}

// New provides protoobject.ObjectServiceServer for the given parameters.
func New(hs Handlers, magicNumber uint32, fsChain FSChain, st Storage, signer ecdsa.PrivateKey, m MetricCollector, ac aclsvc.ACLChecker, rp ACLInfoExtractor) protoobject.ObjectServiceServer {
	return &server{
		handlers:    hs,
		fsChain:     fsChain,
		storage:     st,
		signer:      signer,
		mNumber:     magicNumber,
		metrics:     m,
		aclChecker:  ac,
		reqInfoProc: rp,
	}
}

func (s *server) pushOpExecResult(op stat.Method, err error, startedAt time.Time) {
	s.metrics.HandleOpExecResult(op, err == nil, time.Since(startedAt))
}

func newCurrentProtoVersionMessage() *refs.Version {
	v := version.Current()
	var v2 refsv2.Version
	v.WriteToV2(&v2)
	return v2.ToGRPCMessage().(*refs.Version)
}

func (s *server) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: newCurrentProtoVersionMessage(),
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

type putStream struct {
	signer ecdsa.PrivateKey
	base   *putsvc.Streamer

	cacheReqs bool
	initReq   *protoobject.PutRequest
	chunkReqs []*protoobject.PutRequest

	expBytes, recvBytes uint64 // payload
}

func newIntermediatePutStream(signer ecdsa.PrivateKey, base *putsvc.Streamer) *putStream {
	return &putStream{
		signer: signer,
		base:   base,
	}
}

func (x *putStream) sendToRemoteNode(node client.NodeInfo, c client.MultiAddressClient) error {
	var firstErr error
	nodePub := node.PublicKey()
	addrs := node.AddressGroup()
	for i := range addrs {
		err := putToRemoteNode(c, addrs[i], nodePub, x.initReq, x.chunkReqs)
		if err == nil {
			return nil
		}
		if firstErr == nil {
			firstErr = err
		}
		// TODO: log error
	}
	return firstErr
}

func putToRemoteNode(c client.MultiAddressClient, addr network.Address, nodePub []byte,
	initReq *protoobject.PutRequest, chunkReqs []*protoobject.PutRequest) error {
	var stream protoobject.ObjectService_PutClient
	err := c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
		var err error
		stream, err = protoobject.NewObjectServiceClient(conn).Put(context.TODO()) // FIXME: use proper context
		return err
	})
	if err != nil {
		return fmt.Errorf("stream opening failed: %w", err)
	}

	err = stream.Send(initReq)
	if err != nil {
		internalclient.ReportError(c, err)
		return fmt.Errorf("sending the initial message to stream failed: %w", err)
	}
	for i := range chunkReqs {
		if err := stream.Send(chunkReqs[i]); err != nil {
			internalclient.ReportError(c, err)
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
	resp2 := new(v2object.PutResponse)
	if err := resp2.FromGRPCMessage(resp); err != nil {
		panic(err) // can only fail on wrong type, here it's correct
	}
	if err := signature.VerifyServiceMessage(resp2); err != nil {
		return fmt.Errorf("response verification failed: %w", err)
	}
	if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
		return fmt.Errorf("remote node response: %w", err)
	}
	return nil
}

func (x *putStream) resignRequest(req *protoobject.PutRequest) (*protoobject.PutRequest, error) {
	meta := req.GetMetaHeader()
	req.MetaHeader = &protosession.RequestMetaHeader{
		Ttl:    meta.GetTtl() - 1,
		Origin: meta,
	}
	var req2 v2object.PutRequest
	if err := req2.FromGRPCMessage(req); err != nil {
		panic(err)
	}
	if err := signature.SignServiceMessage(&x.signer, &req2); err != nil {
		return nil, err
	}
	return req2.ToGRPCMessage().(*protoobject.PutRequest), nil
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
		var obj2 v2object.Object
		if err := obj2.FromGRPCMessage(mo); err != nil {
			panic(err)
		}
		obj := object.NewFromV2(&obj2)

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
		c := v.GetChunk()
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
	var id2 refsv2.ObjectID
	id.WriteToV2(&id2)
	return &protoobject.PutResponse{
		Body: &protoobject.PutResponse_Body{
			ObjectId: id2.ToGRPCMessage().(*refs.ObjectID),
		},
	}, nil
}

func (s *server) Put(gStream protoobject.ObjectService_PutServer) error {
	t := time.Now()
	stream, err := s.handlers.Put(gStream.Context())

	defer func() { s.pushOpExecResult(stat.MethodObjectPut, err, t) }()
	if err != nil {
		return err
	}

	var req *protoobject.PutRequest
	var resp *protoobject.PutResponse

	ps := newIntermediatePutStream(s.signer, stream)
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

		putReq := new(v2object.PutRequest)
		if err = putReq.FromGRPCMessage(req); err != nil {
			return err
		}
		if err = signature.VerifyServiceMessage(putReq); err != nil {
			err = s.sendStatusPutResponse(gStream, util.ToRequestSignatureVerificationError(err)) // assign for defer
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

func (s *server) signDeleteResponse(resp *protoobject.DeleteResponse) *protoobject.DeleteResponse {
	return util.SignResponse(&s.signer, resp, v2object.DeleteResponse{})
}

func (s *server) makeStatusDeleteResponse(err error) *protoobject.DeleteResponse {
	return s.signDeleteResponse(&protoobject.DeleteResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

type deleteResponseBody protoobject.DeleteResponse_Body

func (x *deleteResponseBody) SetAddress(addr oid.Address) {
	var addr2 refsv2.Address
	addr.WriteToV2(&addr2)
	x.Tombstone = addr2.ToGRPCMessage().(*refs.Address)
}

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
	var addr2 refsv2.Address
	if err := addr2.FromGRPCMessage(ma); err != nil {
		panic(err)
	}
	err = addr.ReadFromV2(addr2)
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

func (s *server) signHeadResponse(resp *protoobject.HeadResponse) *protoobject.HeadResponse {
	return util.SignResponse(&s.signer, resp, v2object.HeadResponse{})
}

func (s *server) makeStatusHeadResponse(err error) *protoobject.HeadResponse {
	var splitErr *object.SplitInfoError
	if errors.As(err, &splitErr) {
		return s.signHeadResponse(&protoobject.HeadResponse{
			Body: &protoobject.HeadResponse_Body{
				Head: &protoobject.HeadResponse_Body_SplitInfo{
					SplitInfo: splitErr.SplitInfo().ToV2().ToGRPCMessage().(*protoobject.SplitInfo),
				},
			},
		})
	}
	return s.signHeadResponse(&protoobject.HeadResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

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

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusHeadResponse(apistatus.ErrNodeUnderMaintenance), nil
	}

	reqInfo, err := s.reqInfoProc.HeadRequestToInfo(req)
	if err != nil {
		return s.makeStatusHeadResponse(err), nil
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.makeStatusHeadResponse(err), nil
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // needed for defer
		return s.makeStatusHeadResponse(err), nil
	}

	var resp protoobject.HeadResponse
	p, err := convertHeadPrm(s.signer, req, &resp)
	if err != nil {
		return s.makeStatusHeadResponse(err), nil
	}
	err = s.handlers.Head(ctx, p)
	if err != nil {
		return s.makeStatusHeadResponse(err), nil
	}

	err = s.aclChecker.CheckEACL(&resp, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // defer
		return s.makeStatusHeadResponse(err), nil
	}

	return s.signHeadResponse(&resp), nil
}

type headResponse struct {
	short bool
	dst   *protoobject.HeadResponse
}

func (x *headResponse) WriteHeader(hdr *object.Object) error {
	mo := hdr.ToV2().ToGRPCMessage().(*protoobject.Object)
	if x.short {
		mh := mo.GetHeader()
		x.dst.Body = &protoobject.HeadResponse_Body{
			Head: &protoobject.HeadResponse_Body_ShortHeader{
				ShortHeader: &protoobject.ShortHeader{
					Version:         mh.GetVersion(),
					CreationEpoch:   mh.GetCreationEpoch(),
					OwnerId:         mh.GetOwnerId(),
					ObjectType:      mh.GetObjectType(),
					PayloadLength:   mh.GetPayloadLength(),
					PayloadHash:     mh.GetPayloadHash(),
					HomomorphicHash: mh.GetHomomorphicHash(),
				},
			},
		}
		return nil
	}
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
	var addr2 refsv2.Address
	if err := addr2.FromGRPCMessage(ma); err != nil {
		panic(err)
	}
	if err := addr.ReadFromV2(addr2); err != nil {
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
		short: body.MainOnly,
		dst:   resp,
	})
	if cp.LocalOnly() {
		return p, nil
	}

	var onceResign sync.Once
	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.HeadPrm{}, errors.New("missing meta header")
	}
	bID := addr.Object().Marshal()
	p.SetRequestForwarder(func(ctx context.Context, node client.NodeInfo, c client.MultiAddressClient) (*object.Object, error) {
		var err error
		onceResign.Do(func() {
			req.MetaHeader = &protosession.RequestMetaHeader{
				// TODO: #1165 think how to set the other fields
				Ttl:    meta.GetTtl() - 1,
				Origin: meta,
			}
			var req2 v2object.HeadRequest
			if err := req2.FromGRPCMessage(req); err != nil {
				panic(err)
			}
			if err = signature.SignServiceMessage(&signer, &req2); err == nil {
				req = req2.ToGRPCMessage().(*protoobject.HeadRequest)
			}
		})
		if err != nil {
			return nil, err
		}

		var firstErr error
		nodePub := node.PublicKey()
		addrs := node.AddressGroup()
		for i := range addrs {
			hdr, err := getHeaderFromRemoteNode(ctx, c, addrs[i], nodePub, req, bID)
			if err == nil {
				hdr.SetID(addr.Object())
				return hdr, nil
			}
			if firstErr == nil {
				firstErr = err
			}
			// TODO: log error
		}
		return nil, firstErr
	})
	return p, nil
}

func getHeaderFromRemoteNode(ctx context.Context, c client.MultiAddressClient, addr network.Address, nodePub []byte, req *protoobject.HeadRequest,
	bID []byte) (*object.Object, error) {
	var resp *protoobject.HeadResponse
	err := c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
		var err error
		resp, err = protoobject.NewObjectServiceClient(conn).Head(ctx, req)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("sending the request failed: %w", err)
	}

	if err := internal.VerifyResponseKeyV2(nodePub, resp); err != nil {
		return nil, err
	}
	resp2 := new(v2object.HeadResponse)
	if err := resp2.FromGRPCMessage(resp); err != nil {
		panic(err) // can only fail on wrong type, here it's correct
	}
	if err := signature.VerifyServiceMessage(resp2); err != nil {
		return nil, fmt.Errorf("response verification failed: %w", err)
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
		if !req.Body.MainOnly {
			return nil, fmt.Errorf("wrong header part type: expected %T, received %T",
				(*protoobject.HeadResponse_Body_Header)(nil), (*protoobject.HeadResponse_Body_ShortHeader)(nil),
			)
		}
		if v == nil || v.ShortHeader == nil {
			return nil, errors.New("nil short header oneof field")
		}
		h := v.ShortHeader
		hdr = &protoobject.Header{
			Version:         h.Version,
			OwnerId:         h.OwnerId,
			CreationEpoch:   h.CreationEpoch,
			PayloadLength:   h.PayloadLength,
			PayloadHash:     h.PayloadHash,
			ObjectType:      h.ObjectType,
			HomomorphicHash: h.HomomorphicHash,
		}
	case *protoobject.HeadResponse_Body_Header:
		if req.Body.MainOnly {
			return nil, fmt.Errorf("wrong header part type: expected %T, received %T",
				(*protoobject.HeadResponse_Body_Header)(nil), (*protoobject.HeadResponse_Body_ShortHeader)(nil),
			)
		}
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

		var sig2 refsv2.Signature
		if err := sig2.FromGRPCMessage(v.Header.Signature); err != nil {
			panic(err) // can only fail on wrong type, here it's correct
		}
		var sig neofscrypto.Signature
		if err := sig.ReadFromV2(sig2); err != nil {
			return nil, fmt.Errorf("can't read signature: %w", err)
		}
		if !sig.Verify(bID) {
			return nil, errors.New("invalid object ID signature")
		}

		hdr = v.Header.Header
		idSig = v.Header.Signature
	case *protoobject.HeadResponse_Body_SplitInfo:
		if v == nil || v.SplitInfo == nil {
			return nil, errors.New("nil split info oneof field")
		}
		var si2 v2object.SplitInfo
		if err := si2.FromGRPCMessage(v.SplitInfo); err != nil {
			panic(err) // can only fail on wrong type, here it's correct
		}
		si := object.NewSplitInfoFromV2(&si2)
		return nil, object.NewSplitInfoError(si)
	}

	mObj := &protoobject.Object{
		Signature: idSig,
		Header:    hdr,
	}
	objv2 := new(v2object.Object)
	if err := objv2.FromGRPCMessage(mObj); err != nil {
		panic(err) // can only fail on wrong type, here it's correct
	}
	return object.NewFromV2(objv2), nil
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
	var addr2 refsv2.Address
	if err := addr2.FromGRPCMessage(ma); err != nil {
		panic(err)
	}
	if err := addr.ReadFromV2(addr2); err != nil {
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
		var r2 v2object.Range
		if err := r2.FromGRPCMessage(mr[i]); err != nil {
			panic(err)
		}
		rngs[i] = *object.NewRangeFromV2(&r2)
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
			var req2 v2object.GetRangeHashRequest
			if err := req2.FromGRPCMessage(req); err != nil {
				panic(err)
			}
			if err = signature.SignServiceMessage(&signer, &req2); err == nil {
				req = req2.ToGRPCMessage().(*protoobject.GetRangeHashRequest)
			}
		})
		if err != nil {
			return nil, err
		}

		var firstErr error
		nodePub := node.PublicKey()
		addrs := node.AddressGroup()
		for i := range addrs {
			hs, err := getHashesFromRemoteNode(ctx, c, addrs[i], nodePub, req)
			if err == nil {
				return hs, nil
			}
			if firstErr == nil {
				firstErr = err
			}
			// TODO: log error
		}
		return nil, firstErr
	})
	return p, nil
}

func getHashesFromRemoteNode(ctx context.Context, c client.MultiAddressClient, addr network.Address, nodePub []byte,
	req *protoobject.GetRangeHashRequest) ([][]byte, error) {
	var resp *protoobject.GetRangeHashResponse
	err := c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
		var err error
		resp, err = protoobject.NewObjectServiceClient(conn).GetRangeHash(ctx, req)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("GetRangeHash rpc failure: %w", err)
	}

	if err := internal.VerifyResponseKeyV2(nodePub, resp); err != nil {
		return nil, err
	}
	resp2 := new(v2object.GetRangeHashResponse)
	if err := resp2.FromGRPCMessage(resp); err != nil {
		panic(err) // can only fail on wrong type, here it's correct
	}
	if err := signature.VerifyServiceMessage(resp2); err != nil {
		return nil, fmt.Errorf("response verification failed: %w", err)
	}
	if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
		return nil, err
	}
	// TODO: verify number of hashes
	return resp.GetBody().GetHashList(), nil
}

func (s *server) sendGetResponse(stream protoobject.ObjectService_GetServer, resp *protoobject.GetResponse) error {
	return stream.Send(util.SignResponse(&s.signer, resp, v2object.GetResponse{}))
}

func (s *server) sendStatusGetResponse(stream protoobject.ObjectService_GetServer, err error) error {
	var splitErr *object.SplitInfoError
	if errors.As(err, &splitErr) {
		return s.sendGetResponse(stream, &protoobject.GetResponse{
			Body: &protoobject.GetResponse_Body{
				ObjectPart: &protoobject.GetResponse_Body_SplitInfo{
					SplitInfo: splitErr.SplitInfo().ToV2().ToGRPCMessage().(*protoobject.SplitInfo),
				},
			},
		})
	}
	return s.sendGetResponse(stream, &protoobject.GetResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

type getStream struct {
	base    protoobject.ObjectService_GetServer
	srv     *server
	reqInfo aclsvc.RequestInfo
}

func (s *getStream) WriteHeader(hdr *object.Object) error {
	mo := hdr.ToV2().ToGRPCMessage().(*protoobject.Object)
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
	return s.srv.sendGetResponse(s.base, resp)
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
		if err := s.srv.sendGetResponse(s.base, newResp); err != nil {
			return err
		}
	}
	s.srv.metrics.AddGetPayload(len(chunk))
	return nil
}

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

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusGetResponse(gStream, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.GetRequestToInfo(req)
	if err != nil {
		return s.sendStatusGetResponse(gStream, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo) // needed for defer
		return s.sendStatusGetResponse(gStream, err)
	}
	err = s.aclChecker.CheckEACL(req, reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // needed for defer
		return s.sendStatusGetResponse(gStream, err)
	}

	p, err := convertGetPrm(s.signer, req, &getStream{
		base:    gStream,
		srv:     s,
		reqInfo: reqInfo,
	})
	if err != nil {
		return s.sendStatusGetResponse(gStream, err)
	}
	err = s.handlers.Get(gStream.Context(), p)
	if err != nil {
		return s.sendStatusGetResponse(gStream, err)
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
	var addr2 refsv2.Address
	if err := addr2.FromGRPCMessage(ma); err != nil {
		panic(err)
	}
	if err := addr.ReadFromV2(addr2); err != nil {
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
	var onceHdr sync.Once
	var respondedPayload int
	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.Prm{}, errors.New("missing meta header")
	}
	p.SetRequestForwarder(func(ctx context.Context, node client.NodeInfo, c client.MultiAddressClient) (*object.Object, error) {
		var err error
		onceResign.Do(func() {
			req.MetaHeader = &protosession.RequestMetaHeader{
				// TODO: #1165 think how to set the other fields
				Ttl:    meta.GetTtl() - 1,
				Origin: meta,
			}
			var req2 v2object.GetRequest
			if err := req2.FromGRPCMessage(req); err != nil {
				panic(err)
			}
			if err = signature.SignServiceMessage(&signer, &req2); err == nil {
				req = req2.ToGRPCMessage().(*protoobject.GetRequest)
			}
		})
		if err != nil {
			return nil, err
		}

		var firstErr error
		nodePub := node.PublicKey()
		addrs := node.AddressGroup()
		for i := range addrs {
			err := continueGetFromRemoteNode(ctx, c, addrs[i], nodePub, req, stream, &onceHdr, &respondedPayload)
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			if firstErr == nil {
				firstErr = err
			}
			// TODO: log error
		}
		return nil, firstErr
	})
	return p, nil
}

func continueGetFromRemoteNode(ctx context.Context, c client.MultiAddressClient, addr network.Address, nodePub []byte, req *protoobject.GetRequest,
	stream *getStream, onceHdr *sync.Once, respondedPayload *int) error {
	var getStream protoobject.ObjectService_GetClient
	err := c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
		var err error
		// FIXME: context should be cancelled on return from upper func
		getStream, err = protoobject.NewObjectServiceClient(conn).Get(ctx, req)
		return err
	})
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
			internalclient.ReportError(c, err)
			return fmt.Errorf("reading the response failed: %w", err)
		}

		if err = internal.VerifyResponseKeyV2(nodePub, resp); err != nil {
			return err
		}
		resp2 := new(v2object.GetResponse)
		if err := resp2.FromGRPCMessage(resp); err != nil {
			panic(err) // can only fail on wrong type, here it's correct
		}
		if err := signature.VerifyServiceMessage(resp2); err != nil {
			return fmt.Errorf("response verification failed: %w", err)
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
			mo := &protoobject.Object{
				ObjectId:  v.Init.ObjectId,
				Signature: v.Init.Signature,
				Header:    v.Init.Header,
			}
			obj := new(v2object.Object)
			if err := obj.FromGRPCMessage(mo); err != nil {
				panic(err) // can only fail on wrong type, here it's correct
			}
			onceHdr.Do(func() {
				err = stream.WriteHeader(object.NewFromV2(obj))
			})
			if err != nil {
				return fmt.Errorf("could not write object header in Get forwarder: %w", err)
			}
		case *protoobject.GetResponse_Body_Chunk:
			if !headWas {
				return errors.New("incorrect message sequence")
			}
			fullChunk := v.GetChunk()
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
		case *protoobject.GetResponse_Body_SplitInfo:
			if v == nil || v.SplitInfo == nil {
				return errors.New("nil split info oneof field")
			}
			var si2 v2object.SplitInfo
			if err := si2.FromGRPCMessage(v.SplitInfo); err != nil {
				panic(err) // can only fail on wrong type, here it's correct
			}
			si := object.NewSplitInfoFromV2(&si2)
			return object.NewSplitInfoError(si)
		}
	}
}

func (s *server) sendRangeResponse(stream protoobject.ObjectService_GetRangeServer, resp *protoobject.GetRangeResponse) error {
	return stream.Send(util.SignResponse(&s.signer, resp, v2object.GetRangeResponse{}))
}

func (s *server) sendStatusRangeResponse(stream protoobject.ObjectService_GetRangeServer, err error) error {
	var splitErr *object.SplitInfoError
	if errors.As(err, &splitErr) {
		return s.sendRangeResponse(stream, &protoobject.GetRangeResponse{
			Body: &protoobject.GetRangeResponse_Body{
				RangePart: &protoobject.GetRangeResponse_Body_SplitInfo{
					SplitInfo: splitErr.SplitInfo().ToV2().ToGRPCMessage().(*protoobject.SplitInfo),
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
	srv     *server
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
	var addr2 refsv2.Address
	if err := addr2.FromGRPCMessage(ma); err != nil {
		panic(err)
	}
	if err := addr.ReadFromV2(addr2); err != nil {
		return getsvc.RangePrm{}, fmt.Errorf("invalid object address: %w", err)
	}

	rln := body.Range.GetLength()
	if rln == 0 { // includes nil range
		return getsvc.RangePrm{}, errors.New("zero range length")
	}
	if body.Range.Offset+rln <= body.Range.Offset {
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
			var req2 v2object.GetRangeRequest
			if err := req2.FromGRPCMessage(req); err != nil {
				panic(err)
			}
			if err = signature.SignServiceMessage(&signer, &req2); err == nil {
				req = req2.ToGRPCMessage().(*protoobject.GetRangeRequest)
			}
		})
		if err != nil {
			return nil, err
		}

		var firstErr error
		nodePub := node.PublicKey()
		addrs := node.AddressGroup()
		for i := range addrs {
			err := continueRangeFromRemoteNode(ctx, c, addrs[i], nodePub, req, stream, &respondedPayload)
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			if firstErr == nil {
				firstErr = err
			}
			// TODO: log error
		}
		return nil, firstErr
	})
	return p, nil
}

func continueRangeFromRemoteNode(ctx context.Context, c client.MultiAddressClient, addr network.Address, nodePub []byte, req *protoobject.GetRangeRequest,
	stream *rangeStream, respondedPayload *int) error {
	var rangeStream protoobject.ObjectService_GetRangeClient
	err := c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
		var err error
		// FIXME: context should be cancelled on return from upper func
		rangeStream, err = protoobject.NewObjectServiceClient(conn).GetRange(ctx, req)
		return err
	})
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
			internalclient.ReportError(c, err)
			return fmt.Errorf("reading the response failed: %w", err)
		}

		if err = internal.VerifyResponseKeyV2(nodePub, resp); err != nil {
			return err
		}
		resp2 := new(v2object.GetRangeResponse)
		if err := resp2.FromGRPCMessage(resp); err != nil {
			panic(err) // can only fail on wrong type, here it's correct
		}
		if err := signature.VerifyServiceMessage(resp2); err != nil {
			return fmt.Errorf("response verification failed: %w", err)
		}
		if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
			return err
		}

		switch v := resp.GetBody().GetRangePart().(type) {
		default:
			return fmt.Errorf("unexpected range type %T", v)
		case *protoobject.GetRangeResponse_Body_Chunk:
			fullChunk := v.GetChunk()
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
			var si2 v2object.SplitInfo
			if err := si2.FromGRPCMessage(v.SplitInfo); err != nil {
				panic(err) // can only fail on wrong type, here it's correct
			}
			si := object.NewSplitInfoFromV2(&si2)
			return object.NewSplitInfoError(si)
		}
	}
}

func (s *server) sendSearchResponse(stream protoobject.ObjectService_SearchServer, resp *protoobject.SearchResponse) error {
	return stream.Send(util.SignResponse(&s.signer, resp, v2object.SearchResponse{}))
}

func (s *server) sendStatusSearchResponse(stream protoobject.ObjectService_SearchServer, err error) error {
	return s.sendSearchResponse(stream, &protoobject.SearchResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

type searchStream struct {
	base    protoobject.ObjectService_SearchServer
	srv     *server
	reqInfo aclsvc.RequestInfo
}

func (s *searchStream) WriteIDs(ids []oid.ID) error {
	for len(ids) > 0 {
		// gRPC can retain message for some time, this is why we create full new message each time
		r := &protoobject.SearchResponse{
			Body: &protoobject.SearchResponse_Body{},
		}

		cut := maxObjAddrRespAmount
		if cut > len(ids) {
			cut = len(ids)
		}

		r.Body.IdList = make([]*refs.ObjectID, cut)
		var id2 refsv2.ObjectID
		for i := range cut {
			ids[i].WriteToV2(&id2)
			r.Body.IdList[i] = id2.ToGRPCMessage().(*refs.ObjectID)
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
	var id2 refsv2.ContainerID
	if err := id2.FromGRPCMessage(mc); err != nil {
		panic(err)
	}
	if err := id.ReadFromV2(id2); err != nil {
		return searchsvc.Prm{}, fmt.Errorf("invalid container ID: %w", err)
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return searchsvc.Prm{}, err
	}

	mfs := body.GetFilters()
	fs2 := make([]v2object.SearchFilter, len(mfs))
	for i := range mfs {
		if err := fs2[i].FromGRPCMessage(mfs[i]); err != nil {
			panic(err)
		}
	}

	var p searchsvc.Prm
	p.SetCommonParameters(cp)
	p.WithContainerID(id)
	p.WithSearchFilters(object.NewSearchFiltersFromV2(fs2))
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
			var req2 v2object.SearchRequest
			if err := req2.FromGRPCMessage(req); err != nil {
				panic(err)
			}
			if err = signature.SignServiceMessage(&signer, &req2); err == nil {
				req = req2.ToGRPCMessage().(*protoobject.SearchRequest)
			}
		})
		if err != nil {
			return nil, err
		}

		var firstErr error
		nodePub := node.PublicKey()
		addrs := node.AddressGroup()
		for i := range addrs {
			res, err := searchOnRemoteNode(ctx, c, addrs[i], nodePub, req)
			if err == nil {
				return res, nil
			}
			if firstErr == nil {
				firstErr = err
			}
			// TODO: log error
		}
		return nil, firstErr
	})
	return p, nil
}

func searchOnRemoteNode(ctx context.Context, c client.MultiAddressClient, addr network.Address, nodePub []byte, req *protoobject.SearchRequest) ([]oid.ID, error) {
	var searchStream protoobject.ObjectService_SearchClient
	if err := c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
		var err error
		// FIXME: context should be cancelled on return from upper func
		searchStream, err = protoobject.NewObjectServiceClient(conn).Search(ctx, req)
		return err
	}); err != nil {
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
		resp2 := new(v2object.SearchResponse)
		if err := resp2.FromGRPCMessage(resp); err != nil {
			panic(err) // can only fail on wrong type, here it's correct
		}
		if err := signature.VerifyServiceMessage(resp2); err != nil {
			return nil, fmt.Errorf("could not verify %T: %w", resp, err)
		}
		if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
			return nil, fmt.Errorf("remote node response: %w", err)
		}

		chunk := resp.GetBody().GetIdList()
		var id oid.ID
		for i := range chunk {
			var id2 refsv2.ObjectID
			if err := id2.FromGRPCMessage(chunk[i]); err != nil {
				panic(err) // can only fail on wrong type, here it's correct
			}
			if err := id.ReadFromV2(id2); err != nil {
				return nil, fmt.Errorf("invalid object ID: %w", err)
			}
			res = append(res, id)
		}
	}
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

func checkStatus(st *protostatus.Status) error {
	stV2 := new(status.Status)
	if err := stV2.FromGRPCMessage(st); err != nil {
		panic(err) // can only fail on wrong type, here it's correct
	}
	if !status.IsSuccess(stV2.Code()) {
		return apistatus.ErrorFromV2(stV2)
	}

	return nil
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
