package container

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	neoutil "github.com/nspcc-dev/neo-go/pkg/util"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/nns"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	protonetmap "github.com/nspcc-dev/neofs-sdk-go/proto/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// declared in API https://github.com/nspcc-dev/neofs-api/pull/358.
const defaultTxAwaitTimeout = 15 * time.Second

// FSChain provides base non-contract functionality of the FS chain required to
// serve NeoFS API Container service.
type FSChain interface {
	InvokeContainedScript(tx *transaction.Transaction, header *block.Header, _ *trigger.Type, _ *bool) (*result.Invoke, error)

	// HasUserInNNS checks whether user with given address is registered in NNS
	// under the given name.
	HasUserInNNS(name string, addr neoutil.Uint160) (bool, error)
}

// Contract groups ops of the Container contract deployed in the FS chain
// required to serve NeoFS API Container service.
type Contract interface {
	// Put sends transaction creating container with provided credentials. If
	// transaction is accepted for processing, Put waits for it to be successfully
	// executed. Waiting is performed within ctx,
	// [apistatus.ErrContainerAwaitTimeout] is returned on when it is done.
	Put(ctx context.Context, _ container.Container, pub, sig []byte, sessionToken []byte) (cid.ID, error)
	// Get returns container by its ID. Returns [apistatus.ErrContainerNotFound]
	// error if container is missing.
	Get(cid.ID) (container.Container, error)
	// List returns IDs of all container belonging to the given user.
	//
	// Callers do not modify the result.
	List(user.ID) ([]cid.ID, error)
	// PutEACL sends transaction setting container's extended ACL with provided
	// credentials. If transaction is accepted for processing, PutEACL waits for it
	// to be successfully executed. Waiting is performed within ctx,
	// [apistatus.ErrContainerAwaitTimeout] is when it is done.
	PutEACL(ctx context.Context, _ eacl.Table, pub, sig []byte, sessionToken []byte) error
	// GetEACL returns eACL of the container by its ID. Returns
	// [apistatus.ErrEACLNotFound] error if eACL is missing.
	GetEACL(cid.ID) (eacl.Table, error)
	// Delete sends transaction deleting container with provided credentials. If
	// transaction is accepted for processing, Delete waits for it to be
	// successfully executed. Waiting is performed within ctx,
	// [apistatus.ErrContainerAwaitTimeout] is returned when it is done.
	Delete(ctx context.Context, _ cid.ID, pub, sig []byte, sessionToken []byte) error
	// SetAttribute sends transaction setting container attribute with provided
	// credentials. If transaction is accepted for processing, SetAttribute waits
	// for it to be successfully executed. Waiting is performed within ctx,
	// [apistatus.ErrContainerAwaitTimeout] is returned when it is done.
	SetAttribute(ctx context.Context, _ cid.ID, attr, val string, validUntil uint64, pub, sig, sessionToken []byte) error
	// RemoveAttribute sends transaction removing container attribute with provided
	// credentials. If transaction is accepted for processing, RemoveAttribute waits
	// for it to be successfully executed. Waiting is performed within ctx,
	// [apistatus.ErrContainerAwaitTimeout] is returned when it is done.
	RemoveAttribute(ctx context.Context, _ cid.ID, attr string, validUntil uint64, pub, sig, sessionToken []byte) error
}

// NetmapContract represents Netmap contract deployed in the FS chain required
// to serve NeoFS API Container service.
type NetmapContract interface {
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

type historicN3ScriptRunner struct {
	FSChain
	NetmapContract
}

type sessionTokenCommonCheckResult struct {
	token   session.Container
	tokenV2 sessionv2.Token
	err     error
}

// Server provides NeoFS API Container service.
type Server struct {
	protocontainer.UnimplementedContainerServiceServer
	signer    *ecdsa.PrivateKey
	net       netmap.State
	contract  Contract
	resolver  *nns.Resolver
	chainTime TimeProvider
	historicN3ScriptRunner

	sessionTokenCommonCheckCache *lru.Cache[[sha256.Size]byte, sessionTokenCommonCheckResult]
}

// New provides protocontainer.ContainerServiceServer based on specified
// [Contract].
//
// All response messages are signed using specified signer and have current
// epoch in the meta header.
func New(s *ecdsa.PrivateKey, net netmap.State, fsChain FSChain, c Contract, nc NetmapContract, chainTime TimeProvider) *Server {
	sessionTokenCheckCache, err := lru.New[[sha256.Size]byte, sessionTokenCommonCheckResult](1000)
	if err != nil {
		panic(fmt.Errorf("unexpected error in lru.New: %w", err))
	}
	return &Server{
		signer:    s,
		net:       net,
		contract:  c,
		resolver:  nns.NewResolver(fsChain),
		chainTime: chainTime,
		historicN3ScriptRunner: historicN3ScriptRunner{
			FSChain:        fsChain,
			NetmapContract: nc,
		},
		sessionTokenCommonCheckCache: sessionTokenCheckCache,
	}
}

func (s *Server) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: version.Current().ProtoMessage(),
		Epoch:   s.net.CurrentEpoch(),
		Status:  st,
	}
}

// ResetSessionTokenCheckCache resets cache of session token check results.
func (s *Server) ResetSessionTokenCheckCache() {
	s.sessionTokenCommonCheckCache.Purge()
	s.resolver.PurgeCache()
}

// decodes the container session token from the request and checks its
// signature, lifetime and applicability to this operation as per request.
// Returns both nil if token is not attached to the request.
func (s *Server) getVerifiedSessionTokenFromMetaHeader(mh *protosession.RequestMetaHeader, reqVerb session.ContainerVerb, reqCnr cid.ID) (*session.Container, []byte, error) {
	for omh := mh.GetOrigin(); omh != nil; omh = mh.GetOrigin() {
		mh = omh
	}
	m := mh.GetSessionToken()
	if m == nil {
		return nil, nil, nil
	}

	return s.getVerifiedSessionToken(m, reqVerb, reqCnr)
}

func (s *Server) getVerifiedSessionToken(m *protosession.SessionToken, reqVerb session.ContainerVerb, reqCnr cid.ID) (*session.Container, []byte, error) {
	b := make([]byte, m.MarshaledSize())
	m.MarshalStable(b)

	cacheKey := sha256.Sum256(b)
	res, ok := s.sessionTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		res.token, res.err = s.decodeAndVerifySessionTokenCommon(m, b)
		s.sessionTokenCommonCheckCache.Add(cacheKey, res)
	}
	if res.err != nil {
		return nil, nil, res.err
	}

	if err := s.verifySessionTokenAgainstRequest(res.token, reqVerb, reqCnr); err != nil {
		return nil, nil, err
	}

	return &res.token, b, nil
}

type sessionTokenWithEncodedBody struct {
	session.Container
	body []byte
}

func (x sessionTokenWithEncodedBody) SignedData() []byte {
	return x.body
}

func (s *Server) decodeAndVerifySessionTokenCommon(m *protosession.SessionToken, mb []byte) (session.Container, error) {
	var token session.Container
	if err := token.FromProtoMessage(m); err != nil {
		return token, fmt.Errorf("decode: %w", err)
	}

	body, err := iprotobuf.GetFirstBytesField(mb)
	if err != nil {
		return token, fmt.Errorf("get body from calculated session token binary: %w", err)
	}

	if err := icrypto.AuthenticateToken(sessionTokenWithEncodedBody{
		Container: token,
		body:      body,
	}, s.historicN3ScriptRunner); err != nil {
		return token, fmt.Errorf("authenticate: %w", err)
	}

	cur := s.net.CurrentEpoch()
	lt := m.Body.Lifetime // must be checked by ReadFromV2, so NPE is OK here
	if exp := lt.Exp; exp < cur {
		return token, apistatus.ErrSessionTokenExpired
	}
	if iat := lt.Iat; iat > cur {
		return token, fmt.Errorf("token should not be issued yet: IAt: %d, current epoch: %d", iat, cur)
	}
	if nbf := lt.Nbf; nbf > cur {
		return token, fmt.Errorf("token is not valid yet: NBf: %d, current epoch: %d", nbf, cur)
	}

	return token, nil
}

func (s *Server) verifySessionTokenAgainstRequest(token session.Container, reqVerb session.ContainerVerb, reqCnr cid.ID) error {
	if !token.AssertVerb(reqVerb) {
		return errors.New("wrong container session operation")
	}

	if reqCnr.IsZero() {
		return nil
	}

	if !token.AppliedTo(reqCnr) {
		return errors.New("session is not applied to requested container")
	}

	if err := s.checkSessionIssuer(reqCnr, token.Issuer()); err != nil {
		return fmt.Errorf("verify session issuer: %w", err)
	}

	return nil
}

func (s *Server) checkSessionIssuer(id cid.ID, issuer user.ID) error {
	cnr, err := s.contract.Get(id)
	if err != nil {
		return fmt.Errorf("get container by ID: %w", err)
	}

	if owner := cnr.Owner(); issuer != owner {
		return errors.New("session was not issued by the container owner")
	}

	return nil
}

func (s *Server) getVerifiedSessionTokenV2FromMetaHeader(mh *protosession.RequestMetaHeader, reqVerb sessionv2.Verb, reqCnr cid.ID) (*sessionv2.Token, []byte, error) {
	for omh := mh.GetOrigin(); omh != nil; omh = mh.GetOrigin() {
		mh = omh
	}
	m := mh.GetSessionTokenV2()
	if m == nil {
		return nil, nil, nil
	}

	return s.getVerifiedSessionTokenV2(m, reqVerb, reqCnr)
}

func (s *Server) getVerifiedSessionTokenV2(m *protosession.SessionTokenV2, reqVerb sessionv2.Verb, reqCnr cid.ID) (*sessionv2.Token, []byte, error) {
	b := make([]byte, m.MarshaledSize())
	m.MarshalStable(b)

	cacheKey := sha256.Sum256(b)
	res, ok := s.sessionTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		res.tokenV2, res.err = s.decodeAndVerifySessionTokenV2Common(m, b)
	}
	if res.err != nil {
		return nil, nil, res.err
	}

	currentTime := s.chainTime.Now().Round(time.Second)
	if exp := res.tokenV2.Exp(); exp.Before(currentTime) {
		return nil, nil, apistatus.ErrSessionTokenExpired
	}
	if iat := res.tokenV2.Iat(); iat.After(currentTime) {
		return nil, nil, fmt.Errorf("token v2 should not be issued yet: IAt: %s, current time: %s", iat, currentTime)
	}
	if nbf := res.tokenV2.Nbf(); nbf.After(currentTime) {
		return nil, nil, fmt.Errorf("token v2 is not valid yet: NBf: %s, current time: %s", nbf, currentTime)
	}
	if !ok {
		s.sessionTokenCommonCheckCache.Add(cacheKey, res)
	}

	if err := s.verifySessionTokenV2AgainstRequest(res.tokenV2, reqVerb, reqCnr); err != nil {
		return nil, nil, err
	}

	return &res.tokenV2, b, nil
}

type sessionTokenV2WithEncodedBody struct {
	sessionv2.Token
	body []byte
}

func (x sessionTokenV2WithEncodedBody) SignedData() []byte {
	return x.body
}

func (s *Server) decodeAndVerifySessionTokenV2Common(m *protosession.SessionTokenV2, mb []byte) (sessionv2.Token, error) {
	var token sessionv2.Token
	if err := token.FromProtoMessage(m); err != nil {
		return token, fmt.Errorf("decode v2: %w", err)
	}

	if err := token.Validate(s.resolver); err != nil {
		return token, fmt.Errorf("invalid v2 token: %w", err)
	}

	body, err := iprotobuf.GetFirstBytesField(mb)
	if err != nil {
		return token, fmt.Errorf("get body from calculated session token binary: %w", err)
	}

	if err := icrypto.AuthenticateTokenV2(sessionTokenV2WithEncodedBody{
		Token: token,
		body:  body,
	}, s.historicN3ScriptRunner); err != nil {
		return token, fmt.Errorf("authenticate: %w", err)
	}

	return token, nil
}

func (s *Server) verifySessionTokenV2AgainstRequest(token sessionv2.Token, reqVerb sessionv2.Verb, reqCnr cid.ID) error {
	if !token.AssertContainer(reqVerb, reqCnr) {
		return errors.New("v2 session token does not authorize this container operation")
	}

	if reqCnr.IsZero() {
		return nil
	}

	if err := s.checkSessionIssuer(reqCnr, token.OriginalIssuer()); err != nil {
		return fmt.Errorf("verify v2 session issuer: %w", err)
	}

	return nil
}

func (s *Server) makePutResponse(body *protocontainer.PutResponse_Body, err error) (*protocontainer.PutResponse, error) {
	resp := &protocontainer.PutResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *Server) makeFailedPutResponse(err error) (*protocontainer.PutResponse, error) {
	return s.makePutResponse(nil, err)
}

const (
	maxObjectReplicasPerSet = 8
	maxContainerNodesInSet  = 64
	maxContainerNodeSets    = 256
	maxContainerNodes       = 512
)

const defaultBackupFactor = 3

var errInvalidNodeSetDesc = errors.New("invalid node set descriptor")

func verifyStoragePolicy(policy *protonetmap.PlacementPolicy) error {
	if len(policy.Replicas) > maxContainerNodeSets {
		return fmt.Errorf("more than %d node sets", maxContainerNodeSets)
	}
	bf := policy.ContainerBackupFactor
	if bf == 0 {
		bf = defaultBackupFactor
	}
	var cnrNodeCount uint32
	for i := range policy.Replicas {
		if policy.Replicas[i] == nil {
			return fmt.Errorf("nil replica #%d", i)
		}
		if policy.Replicas[i].Count > maxObjectReplicasPerSet {
			return fmt.Errorf("%w #%d: more than %d object replicas", errInvalidNodeSetDesc, i, maxObjectReplicasPerSet)
		}
		var sNum uint32
		if policy.Replicas[i].Selector != "" {
			si := slices.IndexFunc(policy.Selectors, func(s *protonetmap.Selector) bool { return s != nil && s.Name == policy.Replicas[i].Selector })
			if si < 0 {
				return fmt.Errorf("%w #%d: missing selector %q", errInvalidNodeSetDesc, i, policy.Replicas[i].Selector)
			}
			sNum = policy.Selectors[si].Count
		} else {
			sNum = policy.Replicas[i].Count
		}
		nodesInSet := bf * sNum
		if nodesInSet > maxContainerNodesInSet {
			return fmt.Errorf("%w #%d: more than %d nodes", errInvalidNodeSetDesc, i, maxContainerNodesInSet)
		}
		if cnrNodeCount += nodesInSet; cnrNodeCount > maxContainerNodes {
			return fmt.Errorf("more than %d nodes in total", maxContainerNodes)
		}
	}
	return nil
}

// Put forwards container creation request to the underlying [Contract] for
// further processing. If session token is attached, it's verified. Returns ID
// to check request status in the response.
func (s *Server) Put(ctx context.Context, req *protocontainer.PutRequest) (*protocontainer.PutResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedPutResponse(err)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeFailedPutResponse(errors.New("missing container signature"))
	}
	mCnr := reqBody.GetContainer()
	if mCnr == nil {
		return s.makeFailedPutResponse(errors.New("missing container"))
	}

	if mCnr.PlacementPolicy == nil {
		return s.makeFailedPutResponse(errors.New("missing storage policy"))
	}
	if err := verifyStoragePolicy(mCnr.PlacementPolicy); err != nil {
		return s.makeFailedPutResponse(fmt.Errorf("invalid storage policy: %w", err))
	}

	var cnr container.Container
	if err := cnr.FromProtoMessage(mCnr); err != nil {
		return s.makeFailedPutResponse(fmt.Errorf("invalid container: %w", err))
	}

	stV2, tokenBytes, err := s.getVerifiedSessionTokenV2FromMetaHeader(req.GetMetaHeader(), sessionv2.VerbContainerPut, cid.ID{})
	if err != nil {
		return s.makeFailedPutResponse(fmt.Errorf("verify session token v2: %w", err))
	}

	if stV2 == nil {
		st, b, err := s.getVerifiedSessionTokenFromMetaHeader(req.GetMetaHeader(), session.VerbContainerPut, cid.ID{})
		if err != nil {
			return s.makeFailedPutResponse(fmt.Errorf("verify session token: %w", err))
		}
		if st != nil {
			tokenBytes = b
		}
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTxAwaitTimeout)
	defer cancel()

	id, err := s.contract.Put(ctx, cnr, mSig.Key, mSig.Sign, tokenBytes)
	if err != nil && !errors.Is(err, apistatus.ErrContainerAwaitTimeout) {
		return s.makeFailedPutResponse(err)
	}

	respBody := &protocontainer.PutResponse_Body{
		ContainerId: id.ProtoMessage(),
	}
	return s.makePutResponse(respBody, err)
}

func (s *Server) makeDeleteResponse(err error) (*protocontainer.DeleteResponse, error) {
	resp := &protocontainer.DeleteResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

// Delete forwards container removal request to the underlying [Contract] for
// further processing. If session token is attached, it's verified.
func (s *Server) Delete(ctx context.Context, req *protocontainer.DeleteRequest) (*protocontainer.DeleteResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeDeleteResponse(err)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeDeleteResponse(errors.New("missing ID signature"))
	}
	mID := reqBody.GetContainerId()
	if mID == nil {
		return s.makeDeleteResponse(errors.New("missing ID"))
	}

	var id cid.ID
	if err := id.FromProtoMessage(mID); err != nil {
		return s.makeDeleteResponse(fmt.Errorf("invalid ID: %w", err))
	}

	stV2, tokenBytes, err := s.getVerifiedSessionTokenV2FromMetaHeader(req.GetMetaHeader(), sessionv2.VerbContainerDelete, id)
	if err != nil {
		return s.makeDeleteResponse(fmt.Errorf("verify session token v2: %w", err))
	}

	if stV2 == nil {
		st, b, err := s.getVerifiedSessionTokenFromMetaHeader(req.GetMetaHeader(), session.VerbContainerDelete, id)
		if err != nil {
			return s.makeDeleteResponse(fmt.Errorf("verify session token: %w", err))
		}
		if st != nil {
			tokenBytes = b
		}
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTxAwaitTimeout)
	defer cancel()

	err = s.contract.Delete(ctx, id, mSig.Key, mSig.Sign, tokenBytes)

	return s.makeDeleteResponse(err)
}

func (s *Server) makeGetResponse(body *protocontainer.GetResponse_Body, st *protostatus.Status) (*protocontainer.GetResponse, error) {
	resp := &protocontainer.GetResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *Server) makeFailedGetResponse(err error) (*protocontainer.GetResponse, error) {
	return s.makeGetResponse(nil, util.ToStatus(err))
}

// Get requests container from the underlying [Contract] and returns it in the
// response.
func (s *Server) Get(_ context.Context, req *protocontainer.GetRequest) (*protocontainer.GetResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedGetResponse(err)
	}

	mID := req.GetBody().GetContainerId()
	if mID == nil {
		return s.makeFailedGetResponse(errors.New("missing ID"))
	}

	var id cid.ID
	if err := id.FromProtoMessage(mID); err != nil {
		return s.makeFailedGetResponse(fmt.Errorf("invalid ID: %w", err))
	}

	cnr, err := s.contract.Get(id)
	if err != nil {
		return s.makeFailedGetResponse(err)
	}

	body := &protocontainer.GetResponse_Body{
		Container: cnr.ProtoMessage(),
	}
	return s.makeGetResponse(body, nil)
}

func (s *Server) makeListResponse(body *protocontainer.ListResponse_Body, st *protostatus.Status) (*protocontainer.ListResponse, error) {
	resp := &protocontainer.ListResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *Server) makeFailedListResponse(err error) (*protocontainer.ListResponse, error) {
	return s.makeListResponse(nil, util.ToStatus(err))
}

// List lists user containers from the underlying [Contract] and returns their
// IDs in the response.
func (s *Server) List(_ context.Context, req *protocontainer.ListRequest) (*protocontainer.ListResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedListResponse(err)
	}

	mID := req.GetBody().GetOwnerId()
	if mID == nil {
		return s.makeFailedListResponse(errors.New("missing user"))
	}

	var id user.ID
	if len(mID.Value) != user.IDSize || !islices.AllZeros(mID.Value) {
		if err := id.FromProtoMessage(mID); err != nil {
			return s.makeFailedListResponse(fmt.Errorf("invalid user: %w", err))
		}
	}

	cs, err := s.contract.List(id)
	if err != nil {
		return s.makeFailedListResponse(err)
	}

	if len(cs) == 0 {
		return s.makeListResponse(nil, util.StatusOK)
	}

	body := &protocontainer.ListResponse_Body{
		ContainerIds: make([]*refs.ContainerID, len(cs)),
	}
	for i := range cs {
		body.ContainerIds[i] = cs[i].ProtoMessage()
	}
	return s.makeListResponse(body, util.StatusOK)
}

func (s *Server) makeSetEACLResponse(err error) (*protocontainer.SetExtendedACLResponse, error) {
	resp := &protocontainer.SetExtendedACLResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

// SetExtendedACL forwards eACL setting request to the underlying [Contract]
// for further processing. If session token is attached, it's verified.
func (s *Server) SetExtendedACL(ctx context.Context, req *protocontainer.SetExtendedACLRequest) (*protocontainer.SetExtendedACLResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeSetEACLResponse(err)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeSetEACLResponse(errors.New("missing eACL signature"))
	}
	mEACL := reqBody.GetEacl()
	if mEACL == nil {
		return s.makeSetEACLResponse(errors.New("missing eACL"))
	}

	var eACL eacl.Table
	if err := eACL.FromProtoMessage(mEACL); err != nil {
		return s.makeSetEACLResponse(fmt.Errorf("invalid eACL: %w", err))
	}

	cnrID := eACL.GetCID()
	if cnrID.IsZero() {
		return s.makeSetEACLResponse(errors.New("missing container ID in eACL table"))
	}

	stV2, tokenBytes, err := s.getVerifiedSessionTokenV2FromMetaHeader(req.GetMetaHeader(), sessionv2.VerbContainerSetEACL, cnrID)
	if err != nil {
		return s.makeSetEACLResponse(fmt.Errorf("verify session token v2: %w", err))
	}

	if stV2 == nil {
		st, b, err := s.getVerifiedSessionTokenFromMetaHeader(req.GetMetaHeader(), session.VerbContainerSetEACL, cnrID)
		if err != nil {
			return s.makeSetEACLResponse(fmt.Errorf("verify session token: %w", err))
		}
		if st != nil {
			tokenBytes = b
		}
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTxAwaitTimeout)
	defer cancel()

	err = s.contract.PutEACL(ctx, eACL, mSig.Key, mSig.Sign, tokenBytes)

	return s.makeSetEACLResponse(err)
}

func (s *Server) makeGetEACLResponse(body *protocontainer.GetExtendedACLResponse_Body, st *protostatus.Status) (*protocontainer.GetExtendedACLResponse, error) {
	resp := &protocontainer.GetExtendedACLResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *Server) makeFailedGetEACLResponse(err error) (*protocontainer.GetExtendedACLResponse, error) {
	return s.makeGetEACLResponse(nil, util.ToStatus(err))
}

// GetExtendedACL read eACL of the requested container from the underlying
// [Contract] and returns the result in the response.
func (s *Server) GetExtendedACL(_ context.Context, req *protocontainer.GetExtendedACLRequest) (*protocontainer.GetExtendedACLResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedGetEACLResponse(err)
	}

	mID := req.GetBody().GetContainerId()
	if mID == nil {
		return s.makeFailedGetEACLResponse(errors.New("missing ID"))
	}

	var id cid.ID
	if err := id.FromProtoMessage(mID); err != nil {
		return s.makeFailedGetEACLResponse(fmt.Errorf("invalid ID: %w", err))
	}

	eACL, err := s.contract.GetEACL(id)
	if err != nil {
		return s.makeFailedGetEACLResponse(err)
	}

	body := &protocontainer.GetExtendedACLResponse_Body{
		Eacl: eACL.ProtoMessage(),
	}
	return s.makeGetEACLResponse(body, util.StatusOK)
}

func (s *Server) makeSetAttributeResponse(err error) (*protocontainer.SetAttributeResponse, error) {
	return &protocontainer.SetAttributeResponse{
		Status: util.ToStatus(err),
	}, nil
}

func verifySetAttributeRequestBody(body *protocontainer.SetAttributeRequest_Body) error {
	switch {
	case body == nil:
		return errors.New("missing request body")
	case body.Parameters == nil:
		return errors.New("missing parameters")
	case body.Parameters.ContainerId == nil:
		return errors.New("missing container ID")
	case body.Parameters.Attribute == "":
		return errors.New("missing attribute name")
	case body.Parameters.Value == "":
		return errors.New("missing attribute value")
	case body.Signature == nil:
		return errors.New("missing parameters' signature")
	case body.SessionToken != nil && body.SessionTokenV1 != nil:
		return errors.New("both session V1 and V2 tokens set")
	}

	return nil
}

func parseSetAttributeRequestBody(body *protocontainer.SetAttributeRequest_Body) (cid.ID, error) {
	if err := verifySetAttributeRequestBody(body); err != nil {
		return cid.ID{}, err
	}

	var id cid.ID
	if err := id.FromProtoMessage(body.Parameters.ContainerId); err != nil {
		return cid.ID{}, fmt.Errorf("invalid container ID: %w", err)
	}

	return id, nil
}

// SetAttribute forwards attribute setting request to the underlying [Contract]
// for further processing. If session token is attached, it's verified.
func (s *Server) SetAttribute(ctx context.Context, req *protocontainer.SetAttributeRequest) (*protocontainer.SetAttributeResponse, error) {
	if err := neofscrypto.VerifyMessageSignature(req.Body, req.BodySignature, nil); err != nil {
		var e apistatus.SignatureVerification
		e.SetMessage("invalid request signature: " + err.Error())
		return s.makeSetAttributeResponse(e)
	}

	id, err := parseSetAttributeRequestBody(req.Body)
	if err != nil {
		var e apistatus.BadRequest
		e.SetMessage(err.Error())
		return s.makeSetAttributeResponse(e)
	}

	var sessionToken []byte
	if req.Body.SessionToken != nil {
		_, sessionToken, err = s.getVerifiedSessionTokenV2(req.Body.SessionToken, sessionv2.VerbContainerSetAttribute, id)
		if err != nil {
			return s.makeSetAttributeResponse(fmt.Errorf("verify session token V2: %w", err))
		}
	} else if req.Body.SessionTokenV1 != nil {
		_, sessionToken, err = s.getVerifiedSessionToken(req.Body.SessionTokenV1, session.VerbContainerSetAttribute, id)
		if err != nil {
			return s.makeSetAttributeResponse(fmt.Errorf("verify session token V1: %w", err))
		}
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTxAwaitTimeout)
	defer cancel()

	err = s.contract.SetAttribute(ctx, id, req.Body.Parameters.Attribute, req.Body.Parameters.Value, req.Body.Parameters.ValidUntil,
		req.Body.Signature.Key, req.Body.Signature.Sign, sessionToken)

	return s.makeSetAttributeResponse(err)
}

func (s *Server) makeRemoveAttributeResponse(err error) (*protocontainer.RemoveAttributeResponse, error) {
	return &protocontainer.RemoveAttributeResponse{
		Status: util.ToStatus(err),
	}, nil
}

func verifyRemoveAttributeRequestBody(body *protocontainer.RemoveAttributeRequest_Body) error {
	switch {
	case body == nil:
		return errors.New("missing request body")
	case body.Parameters == nil:
		return errors.New("missing parameters")
	case body.Parameters.ContainerId == nil:
		return errors.New("missing container ID")
	case body.Parameters.Attribute == "":
		return errors.New("missing attribute name")
	case body.Signature == nil:
		return errors.New("missing parameters' signature")
	case body.SessionToken != nil && body.SessionTokenV1 != nil:
		return errors.New("both session V1 and V2 tokens set")
	}

	return nil
}

func parseRemoveAttributeRequestBody(body *protocontainer.RemoveAttributeRequest_Body) (cid.ID, error) {
	if err := verifyRemoveAttributeRequestBody(body); err != nil {
		return cid.ID{}, err
	}

	var id cid.ID
	if err := id.FromProtoMessage(body.Parameters.ContainerId); err != nil {
		return cid.ID{}, fmt.Errorf("invalid container ID: %w", err)
	}

	return id, nil
}

// RemoveAttribute forwards attribute removal request to the underlying
// [Contract] for further processing. If session token is attached, it's
// verified.
func (s *Server) RemoveAttribute(ctx context.Context, req *protocontainer.RemoveAttributeRequest) (*protocontainer.RemoveAttributeResponse, error) {
	if err := neofscrypto.VerifyMessageSignature(req.Body, req.BodySignature, nil); err != nil {
		return s.makeRemoveAttributeResponse(err)
	}

	id, err := parseRemoveAttributeRequestBody(req.Body)
	if err != nil {
		var e apistatus.BadRequest
		e.SetMessage(err.Error())
		return s.makeRemoveAttributeResponse(e)
	}

	var sessionToken []byte
	if req.Body.SessionToken != nil {
		_, sessionToken, err = s.getVerifiedSessionTokenV2(req.Body.SessionToken, sessionv2.VerbContainerRemoveAttribute, id)
		if err != nil {
			return s.makeRemoveAttributeResponse(fmt.Errorf("verify session token V2: %w", err))
		}
	} else if req.Body.SessionTokenV1 != nil {
		_, sessionToken, err = s.getVerifiedSessionToken(req.Body.SessionTokenV1, session.VerbContainerRemoveAttribute, id)
		if err != nil {
			return s.makeRemoveAttributeResponse(fmt.Errorf("verify session token V1: %w", err))
		}
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTxAwaitTimeout)
	defer cancel()

	err = s.contract.RemoveAttribute(ctx, id, req.Body.Parameters.Attribute, req.Body.Parameters.ValidUntil,
		req.Body.Signature.Key, req.Body.Signature.Sign, sessionToken)

	return s.makeRemoveAttributeResponse(err)
}
