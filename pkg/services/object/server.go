package object

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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

// ServiceServer is an interface of utility
// serving v2 Object service.
type ServiceServer interface {
	Get(*v2object.GetRequest, GetObjectStream) error
	Put(context.Context) (*putsvc.Streamer, error)
	Head(context.Context, *v2object.HeadRequest) (*v2object.HeadResponse, error)
	Search(context.Context, searchsvc.Prm) error
	Delete(context.Context, deletesvc.Prm) error
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

	// LocalNodeUnderMaintenance checks whether local node is under maintenance
	// according to the network map from FSChain.
	LocalNodeUnderMaintenance() bool
}

// Storage groups ops of the node's object storage required to serve NeoFS API
// Object service.
type Storage interface {
	// VerifyAndStoreObject checks whether given object has correct format and, if
	// so, saves it into the Storage. StoreObject is called only when local node
	// complies with the container's storage policy.
	VerifyAndStoreObject(object.Object) error
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
	srv ServiceServer

	fsChain     FSChain
	storage     Storage
	signer      ecdsa.PrivateKey
	mNumber     uint32
	metrics     MetricCollector
	aclChecker  aclsvc.ACLChecker
	reqInfoProc ACLInfoExtractor
}

// New provides protoobject.ObjectServiceServer for the given parameters.
func New(c ServiceServer, magicNumber uint32, fsChain FSChain, st Storage, signer ecdsa.PrivateKey, m MetricCollector, ac aclsvc.ACLChecker, rp ACLInfoExtractor) protoobject.ObjectServiceServer {
	return &server{
		srv:         c,
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
	stream, err := s.srv.Put(gStream.Context())
	if err != nil {
		return err
	}

	t := time.Now()
	defer func() { s.pushOpExecResult(stat.MethodObjectPut, err, t) }()

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
	err = s.srv.Delete(ctx, p)
	if err != nil {
		return s.makeStatusDeleteResponse(err), nil
	}

	return s.signDeleteResponse(&protoobject.DeleteResponse{Body: &rb}), nil
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

	resp, err := s.srv.Head(ctx, searchReq)
	if err != nil {
		return s.makeStatusHeadResponse(err), nil
	}

	err = s.aclChecker.CheckEACL(resp.ToGRPCMessage().(*protoobject.HeadResponse), reqInfo)
	if err != nil {
		err = eACLErr(reqInfo, err) // defer
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
	srv     *server
	reqInfo aclsvc.RequestInfo
}

func (s *getStreamerV2) Send(resp *v2object.GetResponse) error {
	r := resp.ToGRPCMessage().(*protoobject.GetResponse)
	switch v := r.GetBody().GetObjectPart().(type) {
	case *protoobject.GetResponse_Body_Init_:
		if err := s.srv.aclChecker.CheckEACL(r, s.reqInfo); err != nil {
			return eACLErr(s.reqInfo, err)
		}
	case *protoobject.GetResponse_Body_Chunk:
		for buf := bytes.NewBuffer(v.GetChunk()); buf.Len() > 0; {
			newResp := &protoobject.GetResponse{
				Body: &protoobject.GetResponse_Body{
					ObjectPart: &protoobject.GetResponse_Body_Chunk{
						Chunk: buf.Next(maxRespDataChunkSize),
					},
				},
				MetaHeader:   proto.Clone(r.GetMetaHeader()).(*protosession.ResponseMetaHeader), // TODO: can go w/o cloning?
				VerifyHeader: proto.Clone(r.GetVerifyHeader()).(*protosession.ResponseVerificationHeader),
			}
			if err := s.srv.sendGetResponse(s.ObjectService_GetServer, newResp); err != nil {
				return err
			}
		}
		s.srv.metrics.AddGetPayload(len(v.Chunk))
		return nil
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

	err = s.srv.Get(
		getReq,
		&getStreamerV2{
			ObjectService_GetServer: gStream,
			srv:                     s,
			reqInfo:                 reqInfo,
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
	srv     *server
	reqInfo aclsvc.RequestInfo
}

func (s *getRangeStreamerV2) Send(resp *v2object.GetRangeResponse) error {
	r := resp.ToGRPCMessage().(*protoobject.GetRangeResponse)
	v, ok := r.GetBody().GetRangePart().(*protoobject.GetRangeResponse_Body_Chunk)
	if !ok {
		if err := s.srv.aclChecker.CheckEACL(r, s.reqInfo); err != nil {
			return eACLErr(s.reqInfo, err)
		}
		return s.srv.sendRangeResponse(s.ObjectService_GetRangeServer, r)
	}
	for buf := bytes.NewBuffer(v.GetChunk()); buf.Len() > 0; {
		newResp := &protoobject.GetRangeResponse{
			Body: &protoobject.GetRangeResponse_Body{
				RangePart: &protoobject.GetRangeResponse_Body_Chunk{
					Chunk: buf.Next(maxRespDataChunkSize),
				},
			},
			MetaHeader:   proto.Clone(r.GetMetaHeader()).(*protosession.ResponseMetaHeader), // TODO: can go w/o cloning?
			VerifyHeader: proto.Clone(r.GetVerifyHeader()).(*protosession.ResponseVerificationHeader),
		}
		// TODO: do not check response multiple times
		// TODO: why check it at all?
		if err := s.srv.aclChecker.CheckEACL(newResp, s.reqInfo); err != nil {
			return eACLErr(s.reqInfo, err)
		}
		if err := s.srv.sendRangeResponse(s.ObjectService_GetRangeServer, newResp); err != nil {
			return err
		}
	}
	return nil
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

	err = s.srv.GetRange(
		getRngReq,
		&getRangeStreamerV2{
			ObjectService_GetRangeServer: gStream,
			srv:                          s,
			reqInfo:                      reqInfo,
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
	err = s.srv.Search(gStream.Context(), p)
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
	p.SetRequestForwarder(func(node client.NodeInfo, c client.MultiAddressClient) ([]oid.ID, error) {
		var err error
		onceResign.Do(func() {
			req.MetaHeader = &protosession.RequestMetaHeader{
				// TODO: #1165 think how to set the other fields
				Ttl:    meta.GetTtl() - 1, // FIXME: meta can be nil
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
