package v2

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/internal/chaintime"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoacl "github.com/nspcc-dev/neofs-sdk-go/proto/acl"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	sessionSDKv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

type sessionTokenCommonCheckResult struct {
	token   sessionSDK.Object
	tokenV2 sessionSDKv2.Token
	err     error
}

type bearerTokenCommonCheckResult struct {
	token bearer.Token
	err   error
}

type verifiedSession interface {
	AuthorID() user.ID
	AuthorKey() []byte
}

type verifiedObjectSessionV1 struct {
	author user.ID
	key    []byte
}

func (s verifiedObjectSessionV1) AuthorID() user.ID { return s.author }

func (s verifiedObjectSessionV1) AuthorKey() []byte { return s.key }

type verifiedObjectSessionV2 struct {
	author user.ID
	key    []byte
}

func (s verifiedObjectSessionV2) AuthorID() user.ID { return s.author }

func (s verifiedObjectSessionV2) AuthorKey() []byte { return s.key }

type nnsResolver struct {
	fsChain FSChain

	nnsCache *lru.Cache[string, bool]
}

func (r nnsResolver) HasUser(name string, userID user.ID) (bool, error) {
	cacheKey := name + userID.String()
	hasUser, ok := r.nnsCache.Get(cacheKey)
	if ok {
		return hasUser, nil
	}
	hasUser, err := r.fsChain.HasUserInNNS(name, userID.ScriptHash())
	if err != nil {
		return false, err
	}
	r.nnsCache.Add(cacheKey, hasUser)
	return hasUser, nil
}

// Service checks basic ACL rules.
type Service struct {
	*cfg

	c senderClassifier
	r nnsResolver

	sessionTokenCommonCheckCache *lru.Cache[[sha256.Size]byte, sessionTokenCommonCheckResult]
	bearerTokenCommonCheckCache  *lru.Cache[[sha256.Size]byte, bearerTokenCommonCheckResult]
}

// Option represents Service constructor option.
type Option func(*cfg)

// FSChain provides base non-contract functionality of the FS chain required for
// [Service] to work.
type FSChain interface {
	InvokeContainedScript(tx *transaction.Transaction, header *block.Header, _ *trigger.Type, _ *bool) (*result.Invoke, error)

	// InContainerInLastTwoEpochs checks whether given public key belongs to any SN
	// from the referenced container either in the current or the previous NeoFS
	// epoch.
	InContainerInLastTwoEpochs(_ cid.ID, pub []byte) (bool, error)

	// HasUserInNNS checks whether given user is listed in the NNS domain.
	HasUserInNNS(name string, addr util.Uint160) (bool, error)

	// GetBlockHeader returns FS chain block header by its index.
	GetBlockHeader(ind uint32) (*block.Header, error)

	// BlockCount returns current FS chain block count.
	BlockCount() (res uint32, err error)

	// MsPerBlock returns milliseconds per block protocol configuration.
	MsPerBlock() (res int64, err error)
}

// Netmapper must provide network map information.
type Netmapper interface {
	netmap.Source
	// ServerInContainer checks if current node belongs to requested container.
	// Any unknown state must be returned as `(false, error.New("explanation"))`,
	// not `(false, nil)`.
	ServerInContainer(cid.ID) (bool, error)
	// GetEpochBlock returns FS chain height when given NeoFS epoch was ticked.
	GetEpochBlock(epoch uint64) (uint32, error)
}

type cfg struct {
	log *zap.Logger

	containers container.Source

	irFetcher InnerRingFetcher

	nm Netmapper

	nodeKey *ecdsa.PublicKey

	chainTime chaintime.TimeProvider
}

// WithTimeProvider sets external chain time provider updated from header subscriptions.
func WithTimeProvider(p chaintime.TimeProvider) Option {
	return func(c *cfg) {
		c.chainTime = p
	}
}

func defaultCfg() *cfg {
	return &cfg{
		log: zap.L(),
	}
}

// New is a constructor for object ACL checking service.
func New(fsChain FSChain, opts ...Option) Service {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	panicOnNil := func(v any, name string) {
		if v == nil {
			panic(fmt.Sprintf("ACL service: %s is nil", name))
		}
	}

	panicOnNil(cfg.nm, "netmap client")
	panicOnNil(cfg.irFetcher, "inner Ring fetcher")
	panicOnNil(cfg.containers, "container source")
	panicOnNil(fsChain, "FS chain")

	sessionTokenCheckCache, err := lru.New[[sha256.Size]byte, sessionTokenCommonCheckResult](1000)
	if err != nil {
		panic(fmt.Errorf("unexpected error in lru.New: %w", err))
	}
	bearerTokenCheckCache, err := lru.New[[sha256.Size]byte, bearerTokenCommonCheckResult](1000)
	if err != nil {
		panic(fmt.Errorf("unexpected error in lru.New: %w", err))
	}

	nnsCache, err := lru.New[string, bool](100)
	if err != nil {
		panic(fmt.Errorf("unexpected error in lru.New: %w", err))
	}

	return Service{
		cfg: cfg,
		c: senderClassifier{
			log:       cfg.log,
			innerRing: cfg.irFetcher,
			fsChain:   fsChain,
		},
		r: nnsResolver{
			fsChain:  fsChain,
			nnsCache: nnsCache,
		},
		sessionTokenCommonCheckCache: sessionTokenCheckCache,
		bearerTokenCommonCheckCache:  bearerTokenCheckCache,
	}
}

// ResetTokenCheckCache resets cache of session and bearer tokens' check results.
func (b Service) ResetTokenCheckCache() {
	b.sessionTokenCommonCheckCache.Purge()
	b.bearerTokenCommonCheckCache.Purge()
	b.r.nnsCache.Purge()
}

func (b Service) getVerifiedSessionToken(mh *protosession.RequestMetaHeader, reqVerb sessionSDK.ObjectVerb,
	reqVerbV2 sessionSDKv2.Verb, reqCnr cid.ID, reqObj oid.ID) (verifiedSession, error) {
	for omh := mh.GetOrigin(); omh != nil; omh = mh.GetOrigin() {
		mh = omh
	}

	mV2 := mh.GetSessionTokenV2()
	if mV2 != nil {
		return b.getVerifiedSessionTokenV2(mV2, reqVerbV2, reqCnr, reqObj)
	}

	// Fall back to V1 token
	m := mh.GetSessionToken()
	if m == nil {
		return nil, nil
	}

	mb := make([]byte, m.MarshaledSize())
	m.MarshalStable(mb)

	cacheKey := sha256.Sum256(mb)
	res, ok := b.sessionTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		// TODO: Signed data is used twice - for cache key and to check the signature. Coding can be deduplicated.
		res.token, res.err = b.decodeAndVerifySessionTokenCommon(m)
		b.sessionTokenCommonCheckCache.Add(cacheKey, res)
	}
	if res.err != nil {
		return nil, res.err
	}

	if err := b.verifySessionTokenAgainstRequest(res.token, reqVerb, reqCnr, reqObj); err != nil {
		return nil, err
	}

	sig, ok := res.token.Signature()
	if !ok {
		return nil, errors.New("missing signature in session token")
	}

	return verifiedObjectSessionV1{
		author: res.token.Issuer(),
		key:    sig.PublicKeyBytes(),
	}, nil
}

func (b Service) decodeAndVerifySessionTokenCommon(m *protosession.SessionToken) (sessionSDK.Object, error) {
	var token sessionSDK.Object
	if err := token.FromProtoMessage(m); err != nil {
		return token, fmt.Errorf("invalid session token: %w", err)
	}

	currentEpoch, err := b.nm.Epoch()
	if err != nil {
		return token, errors.New("can't fetch current epoch")
	}
	if token.ExpiredAt(currentEpoch) {
		return token, apistatus.ErrSessionTokenExpired
	}
	if !token.ValidAt(currentEpoch) {
		return token, fmt.Errorf("%s: token is invalid at %d epoch)", invalidRequestMessage, currentEpoch)
	}

	if err := icrypto.AuthenticateToken(&token, historicN3ScriptRunner{
		FSChain:   b.c.fsChain,
		Netmapper: b.nm,
	}); err != nil {
		return token, fmt.Errorf("authenticate session token: %w", err)
	}

	return token, nil
}

func (b Service) verifySessionTokenAgainstRequest(token sessionSDK.Object, reqVerb sessionSDK.ObjectVerb, reqCnr cid.ID, reqObj oid.ID) error {
	if err := assertSessionRelation(token, reqCnr, reqObj); err != nil {
		return err
	}

	if !assertVerb(token, reqVerb) {
		return errInvalidVerb
	}

	return nil
}

// getVerifiedSessionTokenV2 validates and returns V2 session token info.
func (b Service) getVerifiedSessionTokenV2(mV2 *protosession.SessionTokenV2, reqVerb sessionSDKv2.Verb, reqCnr cid.ID, reqObj oid.ID) (verifiedSession, error) {
	mb := make([]byte, mV2.MarshaledSize())
	mV2.MarshalStable(mb)

	cacheKey := sha256.Sum256(mb)
	res, ok := b.sessionTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		res.tokenV2, res.err = b.decodeAndVerifySessionTokenV2Common(mV2)
		b.sessionTokenCommonCheckCache.Add(cacheKey, res)
	}
	if res.err != nil {
		return nil, res.err
	}

	if err := b.verifySessionTokenV2AgainstRequest(res.tokenV2, reqVerb, reqCnr, reqObj); err != nil {
		return nil, err
	}

	var key []byte
	origin := &res.tokenV2
	for origin != nil {
		sig, ok := origin.Signature()
		if !ok {
			return nil, errors.New("missing signature in V2 session token")
		}
		key = sig.PublicKeyBytes()
		origin = origin.Origin()
	}

	return verifiedObjectSessionV2{
		author: res.tokenV2.OriginalIssuer(),
		key:    key,
	}, nil
}

func (b Service) decodeAndVerifySessionTokenV2Common(m *protosession.SessionTokenV2) (sessionSDKv2.Token, error) {
	var token sessionSDKv2.Token
	if err := token.FromProtoMessage(m); err != nil {
		return token, fmt.Errorf("invalid V2 session token: %w", err)
	}

	if err := token.ValidateWithNNS(b.r); err != nil {
		return token, fmt.Errorf("invalid V2 session token: %w", err)
	}

	if b.nodeKey != nil {
		ok, err := token.AssertAuthority(user.NewFromECDSAPublicKey(*b.nodeKey), b.r)
		if err != nil {
			return token, fmt.Errorf("v2 token authority check failed: %w", err)
		}
		if !ok {
			return token, errors.New("v2 token doesn't authorize this node")
		}
	}

	currentTime, err := b.currentChainTime()
	if err != nil {
		return token, err
	}
	currentTime = currentTime.Round(time.Second)
	if !token.ValidAt(currentTime) {
		return token, fmt.Errorf("%s: V2 token is invalid at %s, token iat %s, nbf %s, exp %s", invalidRequestMessage, currentTime, token.Iat(), token.Nbf(), token.Exp())
	}

	return token, nil
}

func (b Service) verifySessionTokenV2AgainstRequest(token sessionSDKv2.Token, reqVerb sessionSDKv2.Verb, reqCnr cid.ID, reqObj oid.ID) error {
	if !reqObj.IsZero() {
		if !token.AssertObject(reqVerb, reqCnr, reqObj) {
			return errors.New("v2 session token does not authorize access to the object")
		}
	} else {
		if !token.AssertVerb(reqVerb, reqCnr) {
			return errInvalidVerb
		}
	}

	return nil
}

// currentChainTime returns chain time preferring external provider.
// If provider is not set, falls back to contract latest block header.
func (b Service) currentChainTime() (time.Time, error) {
	blockTimeMs, err := b.c.fsChain.MsPerBlock()
	if err != nil {
		return time.Time{}, fmt.Errorf("get ms per block protocol config: %w", err)
	}
	durBlockTime := time.Duration(blockTimeMs) * time.Millisecond
	if b.chainTime != nil {
		ct := b.chainTime.Now()
		if !ct.IsZero() {
			return ct.Add(durBlockTime), nil
		}
		// If provider returns zero, fall back to chain.
	}

	idx, err := b.c.fsChain.BlockCount()
	if err != nil {
		return time.Time{}, fmt.Errorf("get FS chain block count: %w", err)
	}

	header, err := b.c.fsChain.GetBlockHeader(idx - 1)
	if err != nil {
		return time.Time{}, fmt.Errorf("get FS chain block header: %w", err)
	}
	return time.UnixMilli(int64(header.Timestamp)).Add(durBlockTime), nil
}

func (b Service) getVerifiedBearerToken(mh *protosession.RequestMetaHeader, reqCnr cid.ID, ownerCnr user.ID, usrSender user.ID) (*bearer.Token, error) {
	for omh := mh.GetOrigin(); omh != nil; omh = mh.GetOrigin() {
		mh = omh
	}
	m := mh.GetBearerToken()
	if m == nil {
		return nil, nil
	}

	mb := make([]byte, m.MarshaledSize())
	m.MarshalStable(mb)

	cacheKey := sha256.Sum256(mb)
	res, ok := b.bearerTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		// TODO: Signed data is used twice - for cache key and to check the signature. Coding can be deduplicated.
		res.token, res.err = b.decodeAndVerifyBearerTokenCommon(m)
		b.bearerTokenCommonCheckCache.Add(cacheKey, res)
	}
	if res.err != nil {
		return nil, res.err
	}

	if err := b.verifyBearerTokenAgainstRequest(res.token, reqCnr, ownerCnr, usrSender); err != nil {
		var errAccessDenied apistatus.ObjectAccessDenied
		errAccessDenied.WriteReason(err.Error())
		return nil, errAccessDenied
	}

	return &res.token, nil
}

func (b Service) decodeAndVerifyBearerTokenCommon(m *protoacl.BearerToken) (bearer.Token, error) {
	var token bearer.Token
	if err := token.FromProtoMessage(m); err != nil {
		return token, fmt.Errorf("invalid bearer token: %w", err)
	}

	currentEpoch, err := b.nm.Epoch()
	if err != nil {
		var errInternal apistatus.ServerInternal
		errInternal.SetMessage(fmt.Sprintf("get current epoch: %s", err))
		return token, errInternal
	}
	if !token.ValidAt(currentEpoch) {
		var errAccessDenied apistatus.ObjectAccessDenied
		errAccessDenied.WriteReason("bearer token has expired")
		return token, errAccessDenied
	}

	if err := icrypto.AuthenticateToken(&token, historicN3ScriptRunner{
		FSChain:   b.c.fsChain,
		Netmapper: b.nm,
	}); err != nil {
		var errAccessDenied apistatus.ObjectAccessDenied
		errAccessDenied.WriteReason(fmt.Sprintf("authenticate bearer token: %s", err))
		return token, errAccessDenied
	}

	return token, nil
}

func (b Service) verifyBearerTokenAgainstRequest(token bearer.Token, reqCnr cid.ID, ownerCnr, reqSender user.ID) error {
	if token.ResolveIssuer() != ownerCnr {
		return errors.New("bearer token issuer differs from the container owner")
	}

	cnr := token.EACLTable().GetCID()
	if !cnr.IsZero() && cnr != reqCnr {
		return errors.New("bearer token was created for another container")
	}

	if !token.AssertUser(reqSender) {
		return errors.New("bearer token owner differs from the request sender")
	}

	return nil
}

// GetRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) GetRequestToInfo(request *protoobject.GetRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	return b.findRequestInfo(request, cnr, acl.OpObjectGet, sessionSDK.VerbObjectGet, sessionSDKv2.VerbObjectGet, *obj)
}

// HeadRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) HeadRequestToInfo(request *protoobject.HeadRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	return b.findRequestInfo(request, cnr, acl.OpObjectHead, sessionSDK.VerbObjectHead, sessionSDKv2.VerbObjectHead, *obj)
}

// SearchRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) SearchRequestToInfo(request *protoobject.SearchRequest) (RequestInfo, error) {
	return b.searchRequestToInfo(request)
}

// SearchV2RequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) SearchV2RequestToInfo(request *protoobject.SearchV2Request) (RequestInfo, error) {
	return b.searchRequestToInfo(request)
}

// unifies V1 and V2 search request processing.
func (b Service) searchRequestToInfo(request interface {
	GetMetaHeader() *protosession.RequestMetaHeader
	GetVerifyHeader() *protosession.RequestVerificationHeader
}) (RequestInfo, error) {
	id, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	return b.findRequestInfo(request, id, acl.OpObjectSearch, sessionSDK.VerbObjectSearch, sessionSDKv2.VerbObjectSearch, oid.ID{})
}

// DeleteRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) DeleteRequestToInfo(request *protoobject.DeleteRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	return b.findRequestInfo(request, cnr, acl.OpObjectDelete, sessionSDK.VerbObjectDelete, sessionSDKv2.VerbObjectDelete, *obj)
}

// RangeRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) RangeRequestToInfo(request *protoobject.GetRangeRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	return b.findRequestInfo(request, cnr, acl.OpObjectRange, sessionSDK.VerbObjectRange, sessionSDKv2.VerbObjectRange, *obj)
}

// HashRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) HashRequestToInfo(request *protoobject.GetRangeHashRequest) (RequestInfo, error) {
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, err
	}

	obj, err := getObjectIDFromRequestBody(request.GetBody())
	if err != nil {
		return RequestInfo{}, err
	}

	return b.findRequestInfo(request, cnr, acl.OpObjectHash, sessionSDK.VerbObjectRangeHash, sessionSDKv2.VerbObjectRangeHash, *obj)
}

var ErrSkipRequest = errors.New("skip request")

// PutRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker]. Returns [ErrSkipRequest] if check should not be performed.
func (b Service) PutRequestToInfo(request *protoobject.PutRequest) (RequestInfo, user.ID, error) {
	body := request.GetBody()
	if body == nil {
		return RequestInfo{}, user.ID{}, errEmptyBody
	}

	part, ok := body.GetObjectPart().(*protoobject.PutRequest_Body_Init_)
	if !ok {
		return RequestInfo{}, user.ID{}, ErrSkipRequest
	}
	if part == nil || part.Init == nil {
		return RequestInfo{}, user.ID{}, errors.New("nil oneof field with heading part")
	}
	cnr, err := getContainerIDFromRequest(request)
	if err != nil {
		return RequestInfo{}, user.ID{}, err
	}

	header := part.Init.Header

	cIDV2 := header.GetContainerId().GetValue()
	var cID cid.ID
	err = cID.Decode(cIDV2)
	if err != nil {
		return RequestInfo{}, user.ID{}, fmt.Errorf("invalid container ID: %w", err)
	}

	inContainer, err := b.nm.ServerInContainer(cID)
	if err != nil {
		return RequestInfo{}, user.ID{}, fmt.Errorf("checking if node in container: %w", err)
	}

	if header.GetSplit() != nil && !inContainer {
		// skip ACL checks for split objects if it is not a container
		// node, since it requires additional object operations (e.g.,
		// requesting the other split parts) that may (or may not) be
		// prohibited by ACL rules; this node is not going to store such
		// objects anyway
		return RequestInfo{}, user.ID{}, ErrSkipRequest
	}

	mOwner := header.GetOwnerId()
	if mOwner == nil {
		return RequestInfo{}, user.ID{}, errors.New("missing object owner")
	}

	var idOwner user.ID
	err = idOwner.FromProtoMessage(mOwner)
	if err != nil {
		return RequestInfo{}, user.ID{}, fmt.Errorf("invalid object owner: %w", err)
	}

	var obj oid.ID
	if part.Init.ObjectId != nil {
		err = obj.FromProtoMessage(part.Init.ObjectId)
		if err != nil {
			return RequestInfo{}, user.ID{}, err
		}
	}

	op, verb, verbv2 := acl.OpObjectPut, sessionSDK.VerbObjectPut, sessionSDKv2.VerbObjectPut
	tombstone := header.GetObjectType() == protoobject.ObjectType_TOMBSTONE
	if tombstone {
		// such objects are specific - saving them is essentially the removal of other
		// objects
		op, verb, verbv2 = acl.OpObjectDelete, sessionSDK.VerbObjectDelete, sessionSDKv2.VerbObjectDelete
		for _, attr := range header.Attributes {
			if attr.Key == object.AttributeAssociatedObject {
				_ = obj.DecodeString(attr.Value)
				break
			}
		}
	}

	reqInfo, err := b.findRequestInfo(request, cnr, op, verb, verbv2, obj)
	if err != nil {
		return RequestInfo{}, user.ID{}, err
	}

	replication := reqInfo.requestRole == acl.RoleContainer && request.GetMetaHeader().GetTtl() == 1
	if tombstone {
		// the only exception when writing tombstone should not be treated as deletion
		// is intra-container replication: container nodes must be able to replicate
		// such objects while deleting is prohibited
		if replication {
			reqInfo.operation = acl.OpObjectPut
		}
	}

	return reqInfo, idOwner, nil
}

func (b Service) findRequestInfo(req interface {
	GetMetaHeader() *protosession.RequestMetaHeader
	GetVerifyHeader() *protosession.RequestVerificationHeader
}, idCnr cid.ID, op acl.Op, verb sessionSDK.ObjectVerb, verb2 sessionSDKv2.Verb, obj oid.ID) (RequestInfo, error) {
	var (
		info      RequestInfo
		metaHdr   = req.GetMetaHeader()
		verifyHdr = req.GetVerifyHeader()
	)
	sTok, err := b.getVerifiedSessionToken(metaHdr, verb, verb2, idCnr, obj)
	if err != nil {
		return info, err
	}

	var reqAuthor user.ID
	var reqAuthorPub []byte
	if sTok != nil {
		reqAuthor = sTok.AuthorID()
		reqAuthorPub = sTok.AuthorKey()
	} else {
		if reqAuthor, reqAuthorPub, err = icrypto.GetRequestAuthor(verifyHdr); err != nil {
			return info, fmt.Errorf("get request author: %w", err)
		}
	}

	cnr, err := b.containers.Get(idCnr)
	if err != nil {
		return info, err
	}

	bTok, err := b.getVerifiedBearerToken(metaHdr, idCnr, cnr.Owner(), reqAuthor)
	if err != nil {
		return info, err
	}

	// find request role and key
	role, err := b.c.classify(idCnr, cnr.Owner(), reqAuthor, reqAuthorPub)
	if err != nil {
		return info, err
	}

	info.basicACL = cnr.BasicACL()
	info.requestRole = role
	info.operation = op
	info.idCnr = idCnr

	// it is assumed that at the moment the key will be valid,
	// otherwise the request would not pass validation
	info.senderKey = reqAuthorPub
	info.senderAccount = &reqAuthor

	// add bearer token if it is present in request
	info.bearer = bTok

	info.srcRequest = req

	if !obj.IsZero() {
		info.obj = &obj
	}

	return info, nil
}
