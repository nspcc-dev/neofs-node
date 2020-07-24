package object

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"strconv"

	"github.com/multiformats/go-multiaddr"
	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended"
	eaclstorage "github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	eaclcheck "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc/eacl"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// RequestTargeter is an interface of request's ACL group calculator.
	RequestTargeter interface {
		Target(context.Context, serviceRequest) requestTarget
	}

	// aclPreProcessor is an implementation of requestPreProcessor interface.
	aclPreProcessor struct {
		log *zap.Logger

		aclInfoReceiver aclInfoReceiver

		reqActionCalc requestActionCalculator

		localStore localstore.Localstore

		extACLSource eaclstorage.Storage

		bearerVerifier bearerTokenVerifier
	}

	// duplicates NetmapClient method, used for testing.
	irKeysReceiver interface {
		InnerRingKeys() ([][]byte, error)
	}

	containerNodesLister interface {
		ContainerNodes(ctx context.Context, cid CID) ([]multiaddr.Multiaddr, error)
		ContainerNodesInfo(ctx context.Context, cid CID, prev int) ([]netmap.Info, error)
	}

	targetFinder struct {
		log *zap.Logger

		irKeysRecv irKeysReceiver
		cnrLister  containerNodesLister
		cnrStorage storage.Storage
	}
)

type objectHeadersSource interface {
	getHeaders() (*Object, bool)
}

type requestActionCalculator interface {
	calculateRequestAction(context.Context, requestActionParams) eacl.Action
}

type aclInfoReceiver struct {
	cnrStorage storage.Storage

	targetFinder RequestTargeter
}

type aclInfo struct {
	rule container.BasicACL

	checkExtended bool

	checkBearer bool

	targetInfo requestTarget
}

type reqActionCalc struct {
	storage eaclstorage.Storage

	log *zap.Logger
}

type serviceRequestInfo struct {
	group eacl.Group

	req serviceRequest

	objHdrSrc objectHeadersSource
}

type requestObjHdrSrc struct {
	req serviceRequest

	ls localstore.Localstore
}

type eaclFromBearer struct {
	eaclstorage.Storage

	bearer service.BearerToken
}

type requestTarget struct {
	group eacl.Group

	ir bool
}

var _ requestPreProcessor = (*aclPreProcessor)(nil)

var errMissingSignatures = errors.New("empty signature list")

func (p *aclPreProcessor) preProcess(ctx context.Context, req serviceRequest) error {
	if req == nil {
		panic(pmEmptyServiceRequest)
	}

	// fetch ACL info
	aclInfo, err := p.aclInfoReceiver.getACLInfo(ctx, req)
	if err != nil {
		p.log.Warn("can't get acl of the container", zap.Stringer("cid", req.CID()))
		return errAccessDenied
	}

	// check basic ACL permissions
	var checkFn func(uint8) bool

	switch aclInfo.targetInfo.group {
	case eacl.GroupUser:
		checkFn = aclInfo.rule.UserAllowed
	case eacl.GroupSystem:
		checkFn = aclInfo.rule.SystemAllowed
	case eacl.GroupOthers:
		checkFn = aclInfo.rule.OthersAllowed
	default:
		panic(fmt.Sprintf("unknown request group (aclPreProcessor): %d", aclInfo.targetInfo.group))
	}

	if requestType := req.Type(); !checkFn(requestACLSection(requestType)) ||
		aclInfo.targetInfo.ir && !allowedInnerRingRequest(requestType) {
		return errAccessDenied
	}

	if aclInfo.targetInfo.group != eacl.GroupSystem &&
		aclInfo.rule.Sticky() &&
		!checkObjectRequestOwnerMatch(req) {
		return errAccessDenied
	}

	if !aclInfo.checkBearer && !aclInfo.checkExtended {
		return nil
	}

	actionParams := requestActionParams{
		eaclSrc: p.extACLSource,
		request: req,
		objHdrSrc: &requestObjHdrSrc{
			req: req,
			ls:  p.localStore,
		},
		group: aclInfo.targetInfo.group,
	}

	if aclInfo.checkBearer {
		bearer := req.GetBearerToken()

		if err := p.bearerVerifier.verifyBearerToken(ctx, req.CID(), bearer); err != nil {
			p.log.Warn("bearer token verification failure",
				zap.String("error", err.Error()),
			)

			return errAccessDenied
		}

		actionParams.eaclSrc = eaclFromBearer{
			bearer: bearer,
		}
	}

	if p.reqActionCalc.calculateRequestAction(ctx, actionParams) != eacl.ActionAllow {
		return errAccessDenied
	}

	return nil
}

func (t *targetFinder) Target(ctx context.Context, req serviceRequest) requestTarget {
	res := requestTarget{
		group: eacl.GroupUnknown,
	}

	ownerID, ownerKey, err := requestOwner(req)
	if err != nil {
		t.log.Warn("could not get request owner",
			zap.String("error", err.Error()),
		)

		return res
	} else if ownerKey == nil {
		t.log.Warn("signature with nil public key detected")
		return res
	}

	// if request from container owner then return GroupUser
	isOwner, err := isContainerOwner(t.cnrStorage, req.CID(), ownerID)
	if err != nil {
		t.log.Warn("can't check container owner", zap.String("err", err.Error()))
		return res
	} else if isOwner {
		res.group = eacl.GroupUser
		return res
	}

	ownerKeyBytes := crypto.MarshalPublicKey(ownerKey)

	// if request from inner ring then return GroupSystem
	irKeyList, err := t.irKeysRecv.InnerRingKeys()
	if err != nil {
		t.log.Warn("could not verify the key belongs to the IR node", zap.String("err", err.Error()))
		return res
	}

	for i := range irKeyList {
		if bytes.Equal(irKeyList[i], ownerKeyBytes) {
			res.group = eacl.GroupSystem
			res.ir = true
			return res
		}
	}

	// if request from current container node then return GroupSystem
	cnr, err := t.cnrLister.ContainerNodesInfo(ctx, req.CID(), 0)
	if err != nil {
		t.log.Warn("can't get current container list", zap.String("err", err.Error()))
		return res
	}

	for i := range cnr {
		if bytes.Equal(cnr[i].PublicKey(), ownerKeyBytes) {
			res.group = eacl.GroupSystem
			return res
		}
	}

	// if request from previous container node then return GroupSystem
	cnr, err = t.cnrLister.ContainerNodesInfo(ctx, req.CID(), 1)
	if err != nil {
		t.log.Warn("can't get previous container list", zap.String("err", err.Error()))
		return res
	}

	for i := range cnr {
		if bytes.Equal(cnr[i].PublicKey(), ownerKeyBytes) {
			res.group = eacl.GroupSystem
			return res
		}
	}

	res.group = eacl.GroupOthers

	// if none of the above return GroupOthers
	return res
}

func checkObjectRequestOwnerMatch(req serviceRequest) bool {
	rt := req.Type()

	// ignore all request types except Put and Delete
	if rt != object.RequestPut && rt != object.RequestDelete {
		return true
	}

	// get request owner
	reqOwner, _, err := requestOwner(req)
	if err != nil {
		return false
	}

	var payloadOwner OwnerID

	// get owner from request payload
	if rt == object.RequestPut {
		obj := req.(transport.PutInfo).GetHead()
		if obj == nil {
			return false
		}

		payloadOwner = obj.GetSystemHeader().OwnerID
	} else {
		payloadOwner = req.(*object.DeleteRequest).OwnerID
	}

	return reqOwner.Equal(payloadOwner)
}

// FIXME: this solution only works with healthy key-to-owner conversion.
func requestOwner(req serviceRequest) (OwnerID, *ecdsa.PublicKey, error) {
	// if session token exists => return its owner
	if token := req.GetSessionToken(); token != nil {
		return token.GetOwnerID(), crypto.UnmarshalPublicKey(token.GetOwnerKey()), nil
	}

	signKeys := req.GetSignKeyPairs()
	if len(signKeys) == 0 {
		return OwnerID{}, nil, errMissingSignatures
	}

	firstKey := signKeys[0].GetPublicKey()
	if firstKey == nil {
		return OwnerID{}, nil, crypto.ErrEmptyPublicKey
	}

	owner, err := refs.NewOwnerID(firstKey)

	return owner, firstKey, err
}

// HeadersOfType returns request or object headers.
func (s serviceRequestInfo) HeadersOfType(typ eacl.HeaderType) ([]eacl.Header, bool) {
	switch typ {
	default:
		return nil, true
	case eacl.HdrTypeRequest:
		return TypedHeaderSourceFromExtendedHeaders(s.req).HeadersOfType(typ)
	case eacl.HdrTypeObjSys, eacl.HdrTypeObjUsr:
		obj, ok := s.objHdrSrc.getHeaders()
		if !ok {
			return nil, false
		}

		return TypedHeaderSourceFromObject(obj).HeadersOfType(typ)
	}
}

// Key returns a binary representation of sender public key.
func (s serviceRequestInfo) Key() []byte {
	_, key, err := requestOwner(s.req)
	if err != nil {
		return nil
	}

	return crypto.MarshalPublicKey(key)
}

// TypeOf returns true of object request type corresponds to passed OperationType.
func (s serviceRequestInfo) OperationType() eacl.OperationType {
	switch t := s.req.Type(); t {
	case object.RequestGet:
		return eacl.OpTypeGet
	case object.RequestPut:
		return eacl.OpTypePut
	case object.RequestHead:
		return eacl.OpTypeHead
	case object.RequestSearch:
		return eacl.OpTypeSearch
	case object.RequestDelete:
		return eacl.OpTypeDelete
	case object.RequestRange:
		return eacl.OpTypeRange
	case object.RequestRangeHash:
		return eacl.OpTypeRangeHash
	default:
		panic(fmt.Sprintf("unknown request type (serviceRequestInfo): %d", t))
	}
}

// Group returns the access group of the request.
func (s serviceRequestInfo) Group() eacl.Group {
	return s.group
}

// CID returns the container identifier of request.
func (s serviceRequestInfo) CID() CID {
	return s.req.CID()
}

func (s requestObjHdrSrc) getHeaders() (*Object, bool) {
	switch s.req.Type() {
	case object.RequestSearch:
		// object header filters is not supported in Search request now
		return nil, true
	case object.RequestPut:
		// for Put we get object headers from request
		return s.req.(transport.PutInfo).GetHead(), true
	default:
		tReq := &transportRequest{
			serviceRequest: s.req,
		}

		// for other requests we get object headers from local storage
		m, err := s.ls.Meta(tReq.GetAddress())
		if err == nil {
			return m.GetObject(), true
		}

		return nil, false
	}
}

type requestActionParams struct {
	eaclSrc eaclstorage.Storage

	request serviceRequest

	objHdrSrc objectHeadersSource

	group eacl.Group
}

func (s reqActionCalc) calculateRequestAction(ctx context.Context, p requestActionParams) eacl.Action {
	// build eACL validator
	validator, err := eaclcheck.NewValidator(p.eaclSrc, s.log)
	if err != nil {
		s.log.Warn("could not build eacl acl validator",
			zap.String("error", err.Error()),
		)

		return eacl.ActionUnknown
	}

	// create RequestInfo instance
	reqInfo := &serviceRequestInfo{
		group:     p.group,
		req:       p.request,
		objHdrSrc: p.objHdrSrc,
	}

	// calculate ACL action
	return validator.CalculateAction(reqInfo)
}

func (s aclInfoReceiver) getACLInfo(ctx context.Context, req serviceRequest) (*aclInfo, error) {
	cnr, err := s.cnrStorage.Get(req.CID())
	if err != nil {
		return nil, err
	}

	rule := cnr.BasicACL()

	isBearer := rule.BearerAllowed(requestACLSection(req.Type()))

	// fetch group from the request
	t := s.targetFinder.Target(ctx, req)

	return &aclInfo{
		rule: rule,

		checkExtended: !rule.Final(),

		targetInfo: t,

		checkBearer: t.group != eacl.GroupSystem && isBearer && req.GetBearerToken() != nil,
	}, nil
}

func (s eaclFromBearer) GetEACL(cid CID) (eaclstorage.Table, error) {
	return eacl.UnmarshalTable(s.bearer.GetACLRules())
}

// returns true if request of type argument is allowed for IR needs (audit).
func allowedInnerRingRequest(t object.RequestType) (res bool) {
	switch t {
	case
		object.RequestSearch,
		object.RequestHead,
		object.RequestRangeHash:
		res = true
	}

	return
}

// returns the index number of request section bits.
func requestACLSection(t object.RequestType) uint8 {
	switch t {
	case object.RequestRangeHash:
		return 0
	case object.RequestRange:
		return 1
	case object.RequestSearch:
		return 2
	case object.RequestDelete:
		return 3
	case object.RequestPut:
		return 4
	case object.RequestHead:
		return 5
	case object.RequestGet:
		return 6
	default:
		panic(fmt.Sprintf("unknown request type (requestACLSection): %d", t))
	}
}

type objectHeaderSource struct {
	obj *Object
}

type typedHeader struct {
	n string
	v string
	t eacl.HeaderType
}

type extendedHeadersWrapper struct {
	hdrSrc service.ExtendedHeadersSource
}

type typedExtendedHeader struct {
	hdr service.ExtendedHeader
}

func newTypedObjSysHdr(name, value string) eacl.TypedHeader {
	return &typedHeader{
		n: name,
		v: value,
		t: eacl.HdrTypeObjSys,
	}
}

// Name is a name field getter.
func (s typedHeader) Name() string {
	return s.n
}

// Value is a value field getter.
func (s typedHeader) Value() string {
	return s.v
}

// HeaderType is a type field getter.
func (s typedHeader) HeaderType() eacl.HeaderType {
	return s.t
}

// TypedHeaderSourceFromObject wraps passed object and returns TypedHeaderSource interface.
func TypedHeaderSourceFromObject(obj *object.Object) extended.TypedHeaderSource {
	return &objectHeaderSource{
		obj: obj,
	}
}

// HeaderOfType gathers object headers of passed type and returns Header list.
//
// If value of some header can not be calculated (e.g. nil eacl header), it does not appear in list.
//
// Always returns true.
func (s objectHeaderSource) HeadersOfType(typ eacl.HeaderType) ([]eacl.Header, bool) {
	if s.obj == nil {
		return nil, true
	}

	var res []eacl.Header

	switch typ {
	case eacl.HdrTypeObjUsr:
		objHeaders := s.obj.GetHeaders()

		res = make([]eacl.Header, 0, len(objHeaders)) // 7 system header fields

		for i := range objHeaders {
			if h := newTypedObjectExtendedHeader(objHeaders[i]); h != nil {
				res = append(res, h)
			}
		}
	case eacl.HdrTypeObjSys:
		res = make([]eacl.Header, 0, 7)

		sysHdr := s.obj.GetSystemHeader()

		created := sysHdr.GetCreatedAt()

		res = append(res,
			// ID
			newTypedObjSysHdr(
				eacl.HdrObjSysNameID,
				sysHdr.ID.String(),
			),

			// CID
			newTypedObjSysHdr(
				eacl.HdrObjSysNameCID,
				sysHdr.CID.String(),
			),

			// OwnerID
			newTypedObjSysHdr(
				eacl.HdrObjSysNameOwnerID,
				sysHdr.OwnerID.String(),
			),

			// Version
			newTypedObjSysHdr(
				eacl.HdrObjSysNameVersion,
				strconv.FormatUint(sysHdr.GetVersion(), 10),
			),

			// PayloadLength
			newTypedObjSysHdr(
				eacl.HdrObjSysNamePayloadLength,
				strconv.FormatUint(sysHdr.GetPayloadLength(), 10),
			),

			// CreatedAt.UnitTime
			newTypedObjSysHdr(
				eacl.HdrObjSysNameCreatedUnix,
				strconv.FormatUint(uint64(created.GetUnixTime()), 10),
			),

			// CreatedAt.Epoch
			newTypedObjSysHdr(
				eacl.HdrObjSysNameCreatedEpoch,
				strconv.FormatUint(created.GetEpoch(), 10),
			),
		)
	}

	return res, true
}

func newTypedObjectExtendedHeader(h object.Header) eacl.TypedHeader {
	val := h.GetValue()
	if val == nil {
		return nil
	}

	res := new(typedHeader)
	res.t = eacl.HdrTypeObjSys

	switch hdr := val.(type) {
	case *object.Header_UserHeader:
		if hdr.UserHeader == nil {
			return nil
		}

		res.t = eacl.HdrTypeObjUsr
		res.n = hdr.UserHeader.GetKey()
		res.v = hdr.UserHeader.GetValue()
	case *object.Header_Link:
		if hdr.Link == nil {
			return nil
		}

		switch hdr.Link.GetType() {
		case object.Link_Previous:
			res.n = eacl.HdrObjSysLinkPrev
		case object.Link_Next:
			res.n = eacl.HdrObjSysLinkNext
		case object.Link_Child:
			res.n = eacl.HdrObjSysLinkChild
		case object.Link_Parent:
			res.n = eacl.HdrObjSysLinkPar
		case object.Link_StorageGroup:
			res.n = eacl.HdrObjSysLinkSG
		default:
			return nil
		}

		res.v = hdr.Link.ID.String()
	default:
		return nil
	}

	return res
}

// TypedHeaderSourceFromExtendedHeaders wraps passed ExtendedHeadersSource and returns TypedHeaderSource interface.
func TypedHeaderSourceFromExtendedHeaders(hdrSrc service.ExtendedHeadersSource) extended.TypedHeaderSource {
	return &extendedHeadersWrapper{
		hdrSrc: hdrSrc,
	}
}

// Name returns the result of Key method.
func (s typedExtendedHeader) Name() string {
	return s.hdr.Key()
}

// Value returns the result of Value method.
func (s typedExtendedHeader) Value() string {
	return s.hdr.Value()
}

// HeaderType always returns HdrTypeRequest.
func (s typedExtendedHeader) HeaderType() eacl.HeaderType {
	return eacl.HdrTypeRequest
}

// TypedHeaders gathers eacl request headers and returns TypedHeader list.
//
// Nil headers are ignored.
//
// Always returns true.
func (s extendedHeadersWrapper) HeadersOfType(typ eacl.HeaderType) ([]eacl.Header, bool) {
	if s.hdrSrc == nil {
		return nil, true
	}

	var res []eacl.Header

	if typ == eacl.HdrTypeRequest {
		hs := s.hdrSrc.ExtendedHeaders()

		res = make([]eacl.Header, 0, len(hs))

		for i := range hs {
			if hs[i] == nil {
				continue
			}

			res = append(res, &typedExtendedHeader{
				hdr: hs[i],
			})
		}
	}

	return res, true
}

func isContainerOwner(storage storage.Storage, cid CID, ownerID OwnerID) (bool, error) {
	cnr, err := storage.Get(cid)
	if err != nil {
		return false, err
	}

	return cnr.OwnerID().Equal(ownerID), nil
}
