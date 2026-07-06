package v2

import (
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
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	nnscore "github.com/nspcc-dev/neofs-node/pkg/core/nns"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/common"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoacl "github.com/nspcc-dev/neofs-sdk-go/proto/acl"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

type sessionTokenCommonCheckResult struct {
	token   sessionSDK.Object
	tokenV2 sessionv2.Token
	err     error
}

type bearerTokenCommonCheckResult struct {
	token bearer.Token
	err   error
}

// Service checks basic ACL rules.
type Service struct {
	*cfg

	c senderClassifier
	r *nnscore.Resolver

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
}

// Netmapper must provide network map information.
type Netmapper interface {
	netmapcore.Source
	// ServerInContainer checks if current node belongs to requested containercore.
	// Any unknown state must be returned as `(false, error.New("explanation"))`,
	// not `(false, nil)`.
	ServerInContainer(cid.ID) (bool, error)
	// GetEpochBlock returns FS chain height when given NeoFS epoch was ticked.
	GetEpochBlock(epoch uint64) (uint32, error)
	// GetEpochBlockByTime returns FS chain height of block index when the latest epoch that
	// started not later than the provided block time came.
	GetEpochBlockByTime(t uint32) (uint32, error)
}

// TimeProvider supplies current FS chain time without calling the chain.
// It should be updated from block header subscriptions and return time
// based on the latest observed header timestamp.
type TimeProvider interface {
	Now() time.Time
}

type cfg struct {
	log *zap.Logger

	containers containercore.Source

	irFetcher InnerRingFetcher

	nm Netmapper

	chainTime TimeProvider
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
	panicOnNil(cfg.chainTime, "chain time provider")

	sessionTokenCheckCache, err := lru.New[[sha256.Size]byte, sessionTokenCommonCheckResult](1000)
	if err != nil {
		panic(fmt.Errorf("unexpected error in lru.New: %w", err))
	}
	bearerTokenCheckCache, err := lru.New[[sha256.Size]byte, bearerTokenCommonCheckResult](1000)
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
		r:                            nnscore.NewResolver(fsChain),
		sessionTokenCommonCheckCache: sessionTokenCheckCache,
		bearerTokenCommonCheckCache:  bearerTokenCheckCache,
	}
}

// ResetTokenCheckCache resets cache of session and bearer tokens' check results.
func (b Service) ResetTokenCheckCache() {
	b.sessionTokenCommonCheckCache.Purge()
	b.bearerTokenCommonCheckCache.Purge()
	b.r.PurgeCache()
}

// VerifySessionV1TokenMessage validates m and converts it into [sessionSDK.Object].
func (b Service) VerifySessionV1TokenMessage(m *protosession.SessionToken, reqVerb sessionSDK.ObjectVerb, reqCnr cid.ID, reqObj oid.ID) (sessionSDK.Object, error) {
	mb := make([]byte, m.MarshaledSize())
	m.MarshalStable(mb)

	cacheKey := sha256.Sum256(mb)
	res, ok := b.sessionTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		res.token, res.err = b.decodeAndVerifySessionTokenCommon(m, mb)
		b.sessionTokenCommonCheckCache.Add(cacheKey, res)
	}
	if res.err != nil {
		return sessionSDK.Object{}, res.err
	}

	if err := b.verifySessionTokenAgainstRequest(res.token, reqVerb, reqCnr, reqObj); err != nil {
		return sessionSDK.Object{}, err
	}

	return res.token, nil
}

func getCredentialsFromSessionV1Token(token sessionSDK.Object) (user.ID, []byte, error) {
	sig, ok := token.Signature()
	if !ok {
		return user.ID{}, nil, errors.New("missing signature in session token")
	}

	return token.Issuer(), sig.PublicKeyBytes(), nil
}

type sessionTokenWithEncodedBody struct {
	sessionSDK.Object
	body []byte
}

func (x sessionTokenWithEncodedBody) SignedData() []byte {
	return x.body
}

func (b Service) decodeAndVerifySessionTokenCommon(m *protosession.SessionToken, mb []byte) (sessionSDK.Object, error) {
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

	body, err := iprotobuf.GetFirstBytesField(mb)
	if err != nil {
		return token, fmt.Errorf("get body from calculated session token binary: %w", err)
	}

	if err := icrypto.AuthenticateToken(sessionTokenWithEncodedBody{
		Object: token,
		body:   body,
	}, historicN3ScriptRunner{
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

// VerifySessionTokenMessage validates mV2 and returns converts it into [sessionv2.Token].
func (b Service) VerifySessionTokenMessage(mV2 *protosession.SessionTokenV2, reqVerb sessionv2.Verb, reqCnr cid.ID) (sessionv2.Token, error) {
	mb := make([]byte, mV2.MarshaledSize())
	mV2.MarshalStable(mb)

	cacheKey := sha256.Sum256(mb)
	res, ok := b.sessionTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		res.tokenV2, res.err = b.decodeAndVerifySessionTokenV2Common(mV2, mb)
	}
	if res.err != nil {
		return sessionv2.Token{}, res.err
	}

	currentTime := b.chainTime.Now().Round(time.Second)
	if res.tokenV2.Exp().Before(currentTime) {
		return sessionv2.Token{}, apistatus.ErrSessionTokenExpired
	}
	if !res.tokenV2.ValidAt(currentTime) {
		return sessionv2.Token{}, fmt.Errorf("%s: V2 token is invalid at %s, token iat %s, nbf %s, exp %s", invalidRequestMessage, currentTime, res.tokenV2.Iat(), res.tokenV2.Nbf(), res.tokenV2.Exp())
	}
	if !ok {
		b.sessionTokenCommonCheckCache.Add(cacheKey, res)
	}

	if !res.tokenV2.AssertVerb(reqVerb, reqCnr) {
		return sessionv2.Token{}, errInvalidVerb
	}

	return res.tokenV2, nil
}

func getCredentialsFromSessionToken(token sessionv2.Token) (user.ID, []byte, error) {
	var key []byte
	origin := &token
	for origin != nil {
		sig, ok := origin.Signature()
		if !ok {
			return user.ID{}, nil, errors.New("missing signature in V2 session token")
		}
		key = sig.PublicKeyBytes()
		origin = origin.Origin()
	}

	return token.OriginalIssuer(), key, nil
}

type sessionTokenV2WithEncodedBody struct {
	sessionv2.Token
	body []byte
}

func (x sessionTokenV2WithEncodedBody) SignedData() []byte {
	return x.body
}

func (b Service) decodeAndVerifySessionTokenV2Common(m *protosession.SessionTokenV2, mb []byte) (sessionv2.Token, error) {
	var token sessionv2.Token
	if err := token.FromProtoMessage(m); err != nil {
		return token, fmt.Errorf("invalid V2 session token: %w", err)
	}

	if err := token.Validate(b.r); err != nil {
		return token, fmt.Errorf("validate V2 session token: %w", err)
	}

	body, err := iprotobuf.GetFirstBytesField(mb)
	if err != nil {
		return token, fmt.Errorf("get body from calculated session token v2 binary: %w", err)
	}

	if err := icrypto.AuthenticateTokenV2(sessionTokenV2WithEncodedBody{
		Token: token,
		body:  body,
	}, historicN3ScriptRunner{
		FSChain:   b.c.fsChain,
		Netmapper: b.nm,
	}); err != nil {
		return token, fmt.Errorf("authenticate session token v2: %w", err)
	}

	return token, nil
}

// VerifyBearerTokenMessage validates m and returns converts it into [bearer.Token].
func (b Service) VerifyBearerTokenMessage(m *protoacl.BearerToken) (bearer.Token, error) {
	mb := make([]byte, m.MarshaledSize())
	m.MarshalStable(mb)

	cacheKey := sha256.Sum256(mb)
	res, ok := b.bearerTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		res.token, res.err = b.decodeAndVerifyBearerTokenCommon(m, mb)
		b.bearerTokenCommonCheckCache.Add(cacheKey, res)
	}
	if res.err != nil {
		return bearer.Token{}, res.err
	}

	return res.token, nil
}

type bearerTokenWithEncodedBody struct {
	bearer.Token
	body []byte
}

func (x bearerTokenWithEncodedBody) SignedData() []byte {
	return x.body
}

func (b Service) decodeAndVerifyBearerTokenCommon(m *protoacl.BearerToken, mb []byte) (bearer.Token, error) {
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

	body, err := iprotobuf.GetFirstBytesField(mb)
	if err != nil {
		return token, fmt.Errorf("get body from calculated bearer token binary: %w", err)
	}

	if err := icrypto.AuthenticateToken(bearerTokenWithEncodedBody{
		Token: token,
		body:  body,
	}, historicN3ScriptRunner{
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
func (b Service) GetRequestToInfo(request *protoobject.GetRequest, cnr cid.ID, tokens common.RequestTokens) (RequestInfo, error) {
	return b.findRequestInfo(request, cnr, acl.OpObjectGet, tokens)
}

// HeadRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) HeadRequestToInfo(request *protoobject.HeadRequest, cnr cid.ID, tokens common.RequestTokens) (RequestInfo, error) {
	return b.findRequestInfo(request, cnr, acl.OpObjectHead, tokens)
}

// SearchV2RequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) SearchV2RequestToInfo(request *protoobject.SearchV2Request, id cid.ID, tokens common.RequestTokens) (RequestInfo, error) {
	return b.findRequestInfo(request, id, acl.OpObjectSearch, tokens)
}

// DeleteRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) DeleteRequestToInfo(request *protoobject.DeleteRequest, cnr cid.ID, tokens common.RequestTokens) (RequestInfo, error) {
	return b.findRequestInfo(request, cnr, acl.OpObjectDelete, tokens)
}

// RangeRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker].
func (b Service) RangeRequestToInfo(request *protoobject.GetRangeRequest, cnr cid.ID, tokens common.RequestTokens) (RequestInfo, error) {
	return b.findRequestInfo(request, cnr, acl.OpObjectRange, tokens)
}

var ErrSkipRequest = errors.New("skip request")

// PutRequestToInfo resolves RequestInfo from the request to check it using
// [ACLChecker]. Returns [ErrSkipRequest] if check should not be performed.
func (b Service) PutRequestToInfo(request *protoobject.PutRequest, initPart *protoobject.PutRequest_Body_Init, cnr cid.ID, op acl.Op, tokens common.RequestTokens) (RequestInfo, user.ID, error) {
	inContainer, err := b.nm.ServerInContainer(cnr)
	if err != nil {
		return RequestInfo{}, user.ID{}, fmt.Errorf("checking if node in container: %w", err)
	}

	header := initPart.Header

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

	reqInfo, err := b.findRequestInfo(request, cnr, op, tokens)
	if err != nil {
		return RequestInfo{}, user.ID{}, err
	}

	replication := reqInfo.RequestRole == acl.RoleContainer && request.GetMetaHeader().GetTtl() == 1
	if op == acl.OpObjectDelete {
		// the only exception when writing tombstone should not be treated as deletion
		// is intra-container replication: container nodes must be able to replicate
		// such objects while deleting is prohibited
		if replication {
			reqInfo.Operation = acl.OpObjectPut
		}
	}

	return reqInfo, idOwner, nil
}

func (b Service) findRequestInfo(req interface {
	GetVerifyHeader() *protosession.RequestVerificationHeader
}, idCnr cid.ID, op acl.Op, tokens common.RequestTokens) (RequestInfo, error) {
	var (
		info         RequestInfo
		reqAuthor    user.ID
		reqAuthorPub []byte
		err          error
	)
	if tokens.Session != nil {
		reqAuthor, reqAuthorPub, err = getCredentialsFromSessionToken(*tokens.Session)
	} else if tokens.SessionV1 != nil {
		reqAuthor, reqAuthorPub, err = getCredentialsFromSessionV1Token(*tokens.SessionV1)
	} else {
		reqAuthor, reqAuthorPub, err = icrypto.GetRequestAuthor(req.GetVerifyHeader())
	}
	if err != nil {
		return info, fmt.Errorf("get request author: %w", err)
	}

	cnr, err := b.containers.Get(idCnr)
	if err != nil {
		return info, err
	}

	if tokens.Bearer != nil {
		if err := b.verifyBearerTokenAgainstRequest(*tokens.Bearer, idCnr, cnr.Owner(), reqAuthor); err != nil {
			var errAccessDenied apistatus.ObjectAccessDenied
			errAccessDenied.WriteReason(err.Error())
			return info, errAccessDenied
		}
	}

	// find request role and key
	role, err := b.c.classify(idCnr, cnr.Owner(), reqAuthor, reqAuthorPub)
	if err != nil {
		return info, err
	}

	info.Container = cnr
	info.RequestRole = role
	info.Operation = op

	// it is assumed that at the moment the key will be valid,
	// otherwise the request would not pass validation
	info.SenderKey = reqAuthorPub
	info.SenderAccount = &reqAuthor

	// add bearer token if it is present in request
	info.Bearer = tokens.Bearer

	info.SrcRequest = req

	return info, nil
}
