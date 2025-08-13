package container

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	protonetmap "github.com/nspcc-dev/neofs-sdk-go/proto/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// FSChain provides base non-contract functionality of the FS chain required to
// serve NeoFS API Container service.
type FSChain interface {
	InvokeContainedScript(tx *transaction.Transaction, header *block.Header, _ *trigger.Type, _ *bool) (*result.Invoke, error)
}

// Contract groups ops of the Container contract deployed in the FS chain
// required to serve NeoFS API Container service.
type Contract interface {
	// Put sends transaction creating container with provided credentials. If
	// accepted, transaction is processed async. Returns container ID to check the
	// creation status.
	Put(_ container.Container, pub, sig []byte, _ *session.Container) (cid.ID, error)
	// Get returns container by its ID. Returns [apistatus.ErrContainerNotFound]
	// error if container is missing.
	Get(cid.ID) (container.Container, error)
	// List returns IDs of all container belonging to the given user.
	//
	// Callers do not modify the result.
	List(user.ID) ([]cid.ID, error)
	// PutEACL sends transaction setting container's eACL with provided credentials.
	// If accepted, transaction is processed async.
	PutEACL(_ eacl.Table, pub, sig []byte, _ *session.Container) error
	// GetEACL returns eACL of the container by its ID. Returns
	// [apistatus.ErrEACLNotFound] error if eACL is missing.
	GetEACL(cid.ID) (eacl.Table, error)
	// Delete sends transaction removing referenced container with provided
	// credentials. If accepted, transaction is processed async.
	Delete(_ cid.ID, pub, sig []byte, _ *session.Container) error
}

// NetmapContract represents Netmap contract deployed in the FS chain required
// to serve NeoFS API Container service.
type NetmapContract interface {
	// GetEpochBlock returns FS chain height when given NeoFS epoch was ticked.
	GetEpochBlock(epoch uint64) (uint32, error)
}

type historicN3ScriptRunner struct {
	FSChain
	NetmapContract
}

type sessionTokenCommonCheckResult struct {
	token session.Container
	err   error
}

type server struct {
	protocontainer.UnimplementedContainerServiceServer
	signer   *ecdsa.PrivateKey
	net      netmap.State
	contract Contract
	historicN3ScriptRunner

	sessionTokenCommonCheckCache *lru.Cache[[sha256.Size]byte, sessionTokenCommonCheckResult]
}

// New provides protocontainer.ContainerServiceServer based on specified
// [Contract].
//
// All response messages are signed using specified signer and have current
// epoch in the meta header.
func New(s *ecdsa.PrivateKey, net netmap.State, fsChain FSChain, c Contract, nc NetmapContract) *server {
	sessionTokenCheckCache, err := lru.New[[sha256.Size]byte, sessionTokenCommonCheckResult](1000)
	if err != nil {
		panic(fmt.Errorf("unexpected error in lru.New: %w", err))
	}
	return &server{
		signer:   s,
		net:      net,
		contract: c,
		historicN3ScriptRunner: historicN3ScriptRunner{
			FSChain:        fsChain,
			NetmapContract: nc,
		},
		sessionTokenCommonCheckCache: sessionTokenCheckCache,
	}
}

func (s *server) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: version.Current().ProtoMessage(),
		Epoch:   s.net.CurrentEpoch(),
		Status:  st,
	}
}

// ResetSessionTokenCheckCache resets cache of session token check results.
func (s *server) ResetSessionTokenCheckCache() {
	s.sessionTokenCommonCheckCache.Purge()
}

// decodes the container session token from the request and checks its
// signature, lifetime and applicability to this operation as per request.
// Returns both nil if token is not attached to the request.
func (s *server) getVerifiedSessionToken(mh *protosession.RequestMetaHeader, reqVerb session.ContainerVerb, reqCnr cid.ID) (*session.Container, error) {
	for omh := mh.GetOrigin(); omh != nil; omh = mh.GetOrigin() {
		mh = omh
	}
	m := mh.GetSessionToken()
	if m == nil {
		return nil, nil
	}

	b := make([]byte, m.MarshaledSize())
	m.MarshalStable(b)

	cacheKey := sha256.Sum256(b)
	res, ok := s.sessionTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		// TODO: Signed data is used twice - for cache key and to check the signature. Coding can be deduplicated.
		res.token, res.err = s.decodeAndVerifySessionTokenCommon(m)
		s.sessionTokenCommonCheckCache.Add(cacheKey, res)
	}
	if res.err != nil {
		return nil, res.err
	}

	if err := s.verifySessionTokenAgainstRequest(res.token, reqVerb, reqCnr); err != nil {
		return nil, err
	}

	return &res.token, nil
}

func (s *server) decodeAndVerifySessionTokenCommon(m *protosession.SessionToken) (session.Container, error) {
	var token session.Container
	if err := token.FromProtoMessage(m); err != nil {
		return token, fmt.Errorf("decode: %w", err)
	}

	if err := icrypto.AuthenticateToken(&token, s.historicN3ScriptRunner); err != nil {
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

func (s *server) verifySessionTokenAgainstRequest(token session.Container, reqVerb session.ContainerVerb, reqCnr cid.ID) error {
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

func (s *server) checkSessionIssuer(id cid.ID, issuer user.ID) error {
	cnr, err := s.contract.Get(id)
	if err != nil {
		return fmt.Errorf("get container by ID: %w", err)
	}

	if owner := cnr.Owner(); issuer != owner {
		return errors.New("session was not issued by the container owner")
	}

	return nil
}

func (s *server) makePutResponse(body *protocontainer.PutResponse_Body, st *protostatus.Status) (*protocontainer.PutResponse, error) {
	resp := &protocontainer.PutResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *server) makeFailedPutResponse(err error) (*protocontainer.PutResponse, error) {
	return s.makePutResponse(nil, util.ToStatus(err))
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
func (s *server) Put(_ context.Context, req *protocontainer.PutRequest) (*protocontainer.PutResponse, error) {
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

	st, err := s.getVerifiedSessionToken(req.GetMetaHeader(), session.VerbContainerPut, cid.ID{})
	if err != nil {
		return s.makeFailedPutResponse(fmt.Errorf("verify session token: %w", err))
	}

	id, err := s.contract.Put(cnr, mSig.Key, mSig.Sign, st)
	if err != nil {
		return s.makeFailedPutResponse(err)
	}

	respBody := &protocontainer.PutResponse_Body{
		ContainerId: id.ProtoMessage(),
	}
	return s.makePutResponse(respBody, util.StatusOK)
}

func (s *server) makeDeleteResponse(err error) (*protocontainer.DeleteResponse, error) {
	resp := &protocontainer.DeleteResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

// Delete forwards container removal request to the underlying [Contract] for
// further processing. If session token is attached, it's verified.
func (s *server) Delete(_ context.Context, req *protocontainer.DeleteRequest) (*protocontainer.DeleteResponse, error) {
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

	st, err := s.getVerifiedSessionToken(req.GetMetaHeader(), session.VerbContainerDelete, id)
	if err != nil {
		return s.makeDeleteResponse(fmt.Errorf("verify session token: %w", err))
	}

	if err := s.contract.Delete(id, mSig.Key, mSig.Sign, st); err != nil {
		return s.makeDeleteResponse(err)
	}

	return s.makeDeleteResponse(util.StatusOKErr)
}

func (s *server) makeGetResponse(body *protocontainer.GetResponse_Body, st *protostatus.Status) (*protocontainer.GetResponse, error) {
	resp := &protocontainer.GetResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *server) makeFailedGetResponse(err error) (*protocontainer.GetResponse, error) {
	return s.makeGetResponse(nil, util.ToStatus(err))
}

// Get requests container from the underlying [Contract] and returns it in the
// response.
func (s *server) Get(_ context.Context, req *protocontainer.GetRequest) (*protocontainer.GetResponse, error) {
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

func (s *server) makeListResponse(body *protocontainer.ListResponse_Body, st *protostatus.Status) (*protocontainer.ListResponse, error) {
	resp := &protocontainer.ListResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *server) makeFailedListResponse(err error) (*protocontainer.ListResponse, error) {
	return s.makeListResponse(nil, util.ToStatus(err))
}

// List lists user containers from the underlying [Contract] and returns their
// IDs in the response.
func (s *server) List(_ context.Context, req *protocontainer.ListRequest) (*protocontainer.ListResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedListResponse(err)
	}

	mID := req.GetBody().GetOwnerId()
	if mID == nil {
		return s.makeFailedListResponse(errors.New("missing user"))
	}

	var id user.ID
	if err := id.FromProtoMessage(mID); err != nil {
		return s.makeFailedListResponse(fmt.Errorf("invalid user: %w", err))
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

func (s *server) makeSetEACLResponse(err error) (*protocontainer.SetExtendedACLResponse, error) {
	resp := &protocontainer.SetExtendedACLResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

// SetExtendedACL forwards eACL setting request to the underlying [Contract]
// for further processing. If session token is attached, it's verified.
func (s *server) SetExtendedACL(_ context.Context, req *protocontainer.SetExtendedACLRequest) (*protocontainer.SetExtendedACLResponse, error) {
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

	st, err := s.getVerifiedSessionToken(req.GetMetaHeader(), session.VerbContainerSetEACL, cnrID)
	if err != nil {
		return s.makeSetEACLResponse(fmt.Errorf("verify session token: %w", err))
	}

	if err := s.contract.PutEACL(eACL, mSig.Key, mSig.Sign, st); err != nil {
		return s.makeSetEACLResponse(err)
	}

	return s.makeSetEACLResponse(util.StatusOKErr)
}

func (s *server) makeGetEACLResponse(body *protocontainer.GetExtendedACLResponse_Body, st *protostatus.Status) (*protocontainer.GetExtendedACLResponse, error) {
	resp := &protocontainer.GetExtendedACLResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp)
	return resp, nil
}

func (s *server) makeFailedGetEACLResponse(err error) (*protocontainer.GetExtendedACLResponse, error) {
	return s.makeGetEACLResponse(nil, util.ToStatus(err))
}

// GetExtendedACL read eACL of the requested container from the underlying
// [Contract] and returns the result in the response.
func (s *server) GetExtendedACL(_ context.Context, req *protocontainer.GetExtendedACLRequest) (*protocontainer.GetExtendedACLResponse, error) {
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
