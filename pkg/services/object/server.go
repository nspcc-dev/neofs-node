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
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
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
	// so, saves it in the Storage. StoreObject is called only when local node
	// complies with the container's storage policy.
	VerifyAndStoreObject(object.Object) error
}

type RequestInfoProcessor interface {
	ProcessPutRequest(*protoobject.PutRequest) (aclsvc.RequestInfo, user.ID, error)
	ProcessDeleteRequest(*protoobject.DeleteRequest) (aclsvc.RequestInfo, error)
	ProcessHeadRequest(*protoobject.HeadRequest) (aclsvc.RequestInfo, error)
	ProcessHashRequest(*protoobject.GetRangeHashRequest) (aclsvc.RequestInfo, error)
	ProcessGetRequest(*protoobject.GetRequest) (aclsvc.RequestInfo, error)
	ProcessRangeRequest(*protoobject.GetRangeRequest) (aclsvc.RequestInfo, error)
	ProcessSearchRequest(*protoobject.SearchRequest) (aclsvc.RequestInfo, error)
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
	reqInfoProc RequestInfoProcessor
}

// New provides protoobject.ObjectServiceServer for the given parameters.
func New(c ServiceServer, magicNumber uint32, fsChain FSChain, st Storage, signer ecdsa.PrivateKey, m MetricCollector, ac aclsvc.ACLChecker, rp RequestInfoProcessor) protoobject.ObjectServiceServer {
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

func (s *server) sendPutResponse(startTime time.Time, stream protoobject.ObjectService_PutServer, err error, resp *protoobject.PutResponse) error {
	resp = util.SignResponse(&s.signer, resp, v2object.PutResponse{})
	s.pushOpExecResult(stat.MethodObjectPut, err, startTime)
	return stream.SendAndClose(resp)
}

func (s *server) sendStatusPutResponse(startTime time.Time, stream protoobject.ObjectService_PutServer, err error) error {
	return s.sendPutResponse(startTime, stream, err, &protoobject.PutResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

// Put opens internal Object service Put stream and overtakes data from gRPC stream to it.
func (s *server) Put(gStream protoobject.ObjectService_PutServer) error {
	startTime := time.Now()
	stream, err := s.srv.Put(gStream.Context())
	if err != nil {
		return s.sendStatusPutResponse(startTime, gStream, err)
	}

	for {
		req, err := gStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				resp, err := stream.CloseAndRecv()
				if err != nil {
					return s.sendStatusPutResponse(startTime, gStream, err)
				}

				return s.sendPutResponse(startTime, gStream, util.StatusOKErr, resp.ToGRPCMessage().(*protoobject.PutResponse))
			}

			return err
		}

		if c := req.GetBody().GetChunk(); c != nil {
			s.metrics.AddPutPayload(len(c))
		}

		putReq := new(v2object.PutRequest)
		if err := putReq.FromGRPCMessage(req); err != nil {
			return err
		}
		if err := signature.VerifyServiceMessage(putReq); err != nil {
			return s.sendStatusPutResponse(startTime, gStream, util.ToRequestSignatureVerificationError(err))
		}

		if s.fsChain.LocalNodeUnderMaintenance() {
			return s.sendStatusPutResponse(startTime, gStream, apistatus.ErrNodeUnderMaintenance)
		}

		if req.Body == nil {
			return errors.New("malformed request: empty body")
		}

		if reqInfo, objOwner, err := s.reqInfoProc.ProcessPutRequest(req); err != nil {
			if !errors.Is(err, aclsvc.ErrSkipRequest) {
				return s.sendStatusPutResponse(startTime, gStream, err)
			}
		} else {
			if !s.aclChecker.CheckBasicACL(reqInfo) || !s.aclChecker.StickyBitCheck(reqInfo, objOwner) {
				return s.sendStatusPutResponse(startTime, gStream, basicACLErr(reqInfo))
			} else if err := s.aclChecker.CheckEACL(req, reqInfo); err != nil {
				return s.sendStatusPutResponse(startTime, gStream, eACLErr(reqInfo, err))
			}
		}

		if err := stream.Send(putReq); err != nil {
			return s.sendStatusPutResponse(startTime, gStream, err)
		}
	}
}

func (s *server) signDeleteResponse(startTime time.Time, err error, resp *protoobject.DeleteResponse) (*protoobject.DeleteResponse, error) {
	resp = util.SignResponse(&s.signer, resp, v2object.DeleteResponse{})
	s.pushOpExecResult(stat.MethodObjectDelete, err, startTime)
	return resp, nil
}

func (s *server) makeStatusDeleteResponse(startTime time.Time, err error) (*protoobject.DeleteResponse, error) {
	return s.signDeleteResponse(startTime, err, &protoobject.DeleteResponse{
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
	if err := delReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	startTime := time.Now()

	if err := signature.VerifyServiceMessage(delReq); err != nil {
		return s.makeStatusDeleteResponse(startTime, err)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusDeleteResponse(startTime, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.ProcessDeleteRequest(req)
	if err != nil {
		return s.makeStatusDeleteResponse(startTime, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		return s.makeStatusDeleteResponse(startTime, basicACLErr(reqInfo))
	} else if err := s.aclChecker.CheckEACL(req, reqInfo); err != nil {
		return s.makeStatusDeleteResponse(startTime, eACLErr(reqInfo, err))
	}

	ma := req.GetBody().GetAddress()
	if ma == nil {
		return s.makeStatusDeleteResponse(startTime, errors.New("missing object address"))
	}
	var addr oid.Address
	var addr2 refsv2.Address
	if err := addr2.FromGRPCMessage(ma); err != nil {
		panic(err)
	}
	if err := addr.ReadFromV2(addr2); err != nil {
		return s.makeStatusDeleteResponse(startTime, fmt.Errorf("invalid object address: %w", err))
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return s.makeStatusDeleteResponse(startTime, err)
	}

	var rb protoobject.DeleteResponse_Body

	var p deletesvc.Prm
	p.SetCommonParameters(cp)
	p.WithAddress(addr)
	p.WithTombstoneAddressTarget((*deleteResponseBody)(&rb))
	if err := s.srv.Delete(ctx, p); err != nil {
		return s.makeStatusDeleteResponse(startTime, err)
	}

	return s.signDeleteResponse(startTime, util.StatusOKErr, &protoobject.DeleteResponse{Body: &rb})
}

func (s *server) signHeadResponse(err error, startTime time.Time, resp *protoobject.HeadResponse) (*protoobject.HeadResponse, error) {
	resp = util.SignResponse(&s.signer, resp, v2object.HeadResponse{})
	s.pushOpExecResult(stat.MethodObjectHead, err, startTime)
	return resp, nil
}

func (s *server) makeStatusHeadResponse(startTime time.Time, err error) (*protoobject.HeadResponse, error) {
	return s.signHeadResponse(err, startTime, &protoobject.HeadResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

// Head converts gRPC HeadRequest message and passes it to internal Object service.
func (s *server) Head(ctx context.Context, req *protoobject.HeadRequest) (*protoobject.HeadResponse, error) {
	searchReq := new(v2object.HeadRequest)
	if err := searchReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	startTime := time.Now()

	if err := signature.VerifyServiceMessage(searchReq); err != nil {
		return s.makeStatusHeadResponse(startTime, err)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusHeadResponse(startTime, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.ProcessHeadRequest(req)
	if err != nil {
		return s.makeStatusHeadResponse(startTime, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		return s.makeStatusHeadResponse(startTime, basicACLErr(reqInfo))
	} else if err := s.aclChecker.CheckEACL(req, reqInfo); err != nil {
		return s.makeStatusHeadResponse(startTime, eACLErr(reqInfo, err))
	}

	resp, err := s.srv.Head(ctx, searchReq)
	if err != nil {
		return s.makeStatusHeadResponse(startTime, err)
	}

	if err := s.aclChecker.CheckEACL(resp.ToGRPCMessage().(*protoobject.HeadResponse), reqInfo); err != nil {
		return s.makeStatusHeadResponse(startTime, eACLErr(reqInfo, err))
	}

	return s.signHeadResponse(nil, startTime, resp.ToGRPCMessage().(*protoobject.HeadResponse))
}

func (s *server) signHashResponse(startTime time.Time, err error, resp *protoobject.GetRangeHashResponse) (*protoobject.GetRangeHashResponse, error) {
	resp = util.SignResponse(&s.signer, resp, v2object.GetRangeHashResponse{})
	s.pushOpExecResult(stat.MethodObjectHash, err, startTime)
	return resp, nil
}

func (s *server) makeStatusHashResponse(startTime time.Time, err error) (*protoobject.GetRangeHashResponse, error) {
	return s.signHashResponse(startTime, err, &protoobject.GetRangeHashResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
}

// GetRangeHash converts gRPC GetRangeHashRequest message and passes it to internal Object service.
func (s *server) GetRangeHash(ctx context.Context, req *protoobject.GetRangeHashRequest) (*protoobject.GetRangeHashResponse, error) {
	hashRngReq := new(v2object.GetRangeHashRequest)
	if err := hashRngReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	startTime := time.Now()

	if err := signature.VerifyServiceMessage(hashRngReq); err != nil {
		return s.makeStatusHashResponse(startTime, err)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.makeStatusHashResponse(startTime, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.ProcessHashRequest(req)
	if err != nil {
		return s.makeStatusHashResponse(startTime, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		return s.makeStatusHashResponse(startTime, basicACLErr(reqInfo))
	} else if err := s.aclChecker.CheckEACL(req, reqInfo); err != nil {
		return s.makeStatusHashResponse(startTime, eACLErr(reqInfo, err))
	}

	resp, err := s.srv.GetRangeHash(ctx, hashRngReq)
	if err != nil {
		return s.makeStatusHashResponse(startTime, err)
	}

	return s.signHashResponse(startTime, nil, resp.ToGRPCMessage().(*protoobject.GetRangeHashResponse))
}

func (s *server) sendGetResponse(stream protoobject.ObjectService_GetServer, resp *protoobject.GetResponse) error {
	resp = util.SignResponse(&s.signer, resp, v2object.GetResponse{})
	return stream.Send(resp)
}

func (s *server) sendStatusGetResponse(startTime time.Time, stream protoobject.ObjectService_GetServer, err error) error {
	sendErr := s.sendGetResponse(stream, &protoobject.GetResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
	s.pushOpExecResult(stat.MethodObjectGet, err, startTime)
	return sendErr
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
	if err := getReq.FromGRPCMessage(req); err != nil {
		return err
	}

	startTime := time.Now()

	if err := signature.VerifyServiceMessage(getReq); err != nil {
		return s.sendStatusGetResponse(startTime, gStream, err)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusGetResponse(startTime, gStream, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.ProcessGetRequest(req)
	if err != nil {
		return s.sendStatusGetResponse(startTime, gStream, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		return s.sendStatusGetResponse(startTime, gStream, basicACLErr(reqInfo))
	} else if err := s.aclChecker.CheckEACL(req, reqInfo); err != nil {
		return s.sendStatusGetResponse(startTime, gStream, eACLErr(reqInfo, err))
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
		return s.sendStatusGetResponse(startTime, gStream, err)
	}
	s.pushOpExecResult(stat.MethodObjectGet, nil, startTime)
	return nil
}

func (s *server) sendRangeResponse(stream protoobject.ObjectService_GetRangeServer, resp *protoobject.GetRangeResponse) error {
	resp = util.SignResponse(&s.signer, resp, v2object.GetRangeResponse{})
	return stream.Send(resp)
}

func (s *server) sendStatusRangeResponse(startTime time.Time, stream protoobject.ObjectService_GetRangeServer, err error) error {
	sendErr := s.sendRangeResponse(stream, &protoobject.GetRangeResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
	s.pushOpExecResult(stat.MethodObjectRange, err, startTime)
	return sendErr
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
	if err := getRngReq.FromGRPCMessage(req); err != nil {
		return err
	}

	startTime := time.Now()

	if err := signature.VerifyServiceMessage(getRngReq); err != nil {
		return s.sendStatusRangeResponse(startTime, gStream, err)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusRangeResponse(startTime, gStream, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.ProcessRangeRequest(req)
	if err != nil {
		return s.sendStatusRangeResponse(startTime, gStream, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		return s.sendStatusRangeResponse(startTime, gStream, basicACLErr(reqInfo))
	} else if err := s.aclChecker.CheckEACL(req, reqInfo); err != nil {
		return s.sendStatusRangeResponse(startTime, gStream, eACLErr(reqInfo, err))
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
		return s.sendStatusRangeResponse(startTime, gStream, err)
	}
	s.pushOpExecResult(stat.MethodObjectRange, nil, startTime)
	return nil
}

func (s *server) sendSearchResponse(stream protoobject.ObjectService_SearchServer, resp *protoobject.SearchResponse) error {
	resp = util.SignResponse(&s.signer, resp, v2object.SearchResponse{})
	return stream.Send(resp)
}

func (s *server) sendStatusSearchResponse(startTime time.Time, stream protoobject.ObjectService_SearchServer, err error) error {
	sendErr := s.sendSearchResponse(stream, &protoobject.SearchResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	})
	s.pushOpExecResult(stat.MethodObjectSearch, err, startTime)
	return sendErr
}

type searchStreamerV2 struct {
	protoobject.ObjectService_SearchServer
	srv     *server
	reqInfo aclsvc.RequestInfo
}

func (s *searchStreamerV2) Send(resp *v2object.SearchResponse) error {
	r := resp.ToGRPCMessage().(*protoobject.SearchResponse)
	ids := r.GetBody().GetIdList()
	for len(ids) > 0 {
		newResp := &protoobject.SearchResponse{
			Body:         &protoobject.SearchResponse_Body{},
			MetaHeader:   proto.Clone(r.GetMetaHeader()).(*protosession.ResponseMetaHeader), // TODO: can go w/o cloning?
			VerifyHeader: proto.Clone(r.GetVerifyHeader()).(*protosession.ResponseVerificationHeader),
		}

		cut := maxObjAddrRespAmount
		if cut > len(ids) {
			cut = len(ids)
		}

		newResp.Body.IdList = ids[:cut]
		// TODO: do not check response multiple times
		// TODO: why check it at all?
		if err := s.srv.aclChecker.CheckEACL(r, s.reqInfo); err != nil {
			return eACLErr(s.reqInfo, err)
		}
		if err := s.srv.sendSearchResponse(s.ObjectService_SearchServer, r); err != nil {
			return err
		}

		ids = ids[cut:]
	}
	return nil
}

// Search converts gRPC SearchRequest message and server-side stream and overtakes its data
// to gRPC stream.
func (s *server) Search(req *protoobject.SearchRequest, gStream protoobject.ObjectService_SearchServer) error {
	searchReq := new(v2object.SearchRequest)
	if err := searchReq.FromGRPCMessage(req); err != nil {
		return err
	}

	startTime := time.Now()

	if err := signature.VerifyServiceMessage(searchReq); err != nil {
		return s.sendStatusSearchResponse(startTime, gStream, err)
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		return s.sendStatusSearchResponse(startTime, gStream, apistatus.ErrNodeUnderMaintenance)
	}

	reqInfo, err := s.reqInfoProc.ProcessSearchRequest(req)
	if err != nil {
		return s.sendStatusSearchResponse(startTime, gStream, err)
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		return s.sendStatusSearchResponse(startTime, gStream, basicACLErr(reqInfo))
	} else if err := s.aclChecker.CheckEACL(req, reqInfo); err != nil {
		return s.sendStatusSearchResponse(startTime, gStream, eACLErr(reqInfo, err))
	}

	err = s.srv.Search(
		searchReq,
		&searchStreamerV2{
			ObjectService_SearchServer: gStream,
			srv:                        s,
			reqInfo:                    reqInfo,
		},
	)
	if err != nil {
		return s.sendStatusSearchResponse(startTime, gStream, err)
	}
	s.pushOpExecResult(stat.MethodObjectSearch, nil, startTime)
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
	var deleted []oid.ID
	var locked []oid.ID
	switch o.Type() {
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

	firstMeta := objectcore.EncodeReplicationMetaInfo(o.GetContainerID(), o.GetID(), o.PayloadSize(), deleted, locked, firstBlock, s.mNumber)
	secondMeta := objectcore.EncodeReplicationMetaInfo(o.GetContainerID(), o.GetID(), o.PayloadSize(), deleted, locked, secondBlock, s.mNumber)
	thirdMeta := objectcore.EncodeReplicationMetaInfo(o.GetContainerID(), o.GetID(), o.PayloadSize(), deleted, locked, thirdBlock, s.mNumber)

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
