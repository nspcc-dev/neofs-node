package container

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"slices"

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

type server struct {
	protocontainer.UnimplementedContainerServiceServer
	signer   *ecdsa.PrivateKey
	net      netmap.State
	contract Contract
	historicN3ScriptRunner
}

// New provides protocontainer.ContainerServiceServer based on specified
// [Contract].
//
// All response messages are signed using specified signer and have current
// epoch in the meta header.
func New(s *ecdsa.PrivateKey, net netmap.State, fsChain FSChain, c Contract, nc NetmapContract) protocontainer.ContainerServiceServer {
	return &server{
		signer:   s,
		net:      net,
		contract: c,
		historicN3ScriptRunner: historicN3ScriptRunner{
			FSChain:        fsChain,
			NetmapContract: nc,
		},
	}
}

func (s *server) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: version.Current().ProtoMessage(),
		Epoch:   s.net.CurrentEpoch(),
		Status:  st,
	}
}

// decodes the container session token from the request and checks its
// signature, lifetime and applicability to this operation as per request.
// Returns both nil if token is not attached to the request.
func (s *server) getVerifiedSessionToken(req interface {
	GetMetaHeader() *protosession.RequestMetaHeader
}) (*session.Container, error) {
	mh := req.GetMetaHeader()
	for omh := mh.GetOrigin(); omh != nil; omh = mh.GetOrigin() {
		mh = omh
	}
	m := mh.GetSessionToken()
	if m == nil {
		return nil, nil
	}

	var token session.Container
	if err := token.FromProtoMessage(m); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	if err := icrypto.AuthenticateToken(&token, s.historicN3ScriptRunner); err != nil {
		return nil, fmt.Errorf("authenticate: %w", err)
	}

	var expVerb session.ContainerVerb
	switch req.(type) {
	default:
		panic(fmt.Sprintf("unexpected request type %T", req))
	case *protocontainer.PutRequest:
		expVerb = session.VerbContainerPut
	case *protocontainer.DeleteRequest:
		expVerb = session.VerbContainerDelete
	case *protocontainer.SetExtendedACLRequest:
		expVerb = session.VerbContainerSetEACL
	}
	if !token.AssertVerb(expVerb) {
		// must be checked by ReadFromV2, so NPE is OK here
		verb := m.Body.Context.(*protosession.SessionToken_Body_Container).Container.Verb
		return nil, fmt.Errorf("wrong container session operation: %s", verb)
	}

	cur := s.net.CurrentEpoch()
	lt := m.Body.Lifetime // must be checked by ReadFromV2, so NPE is OK here
	if exp := lt.Exp; exp < cur {
		return nil, apistatus.ErrSessionTokenExpired
	}
	if iat := lt.Iat; iat > cur {
		return nil, fmt.Errorf("token should not be issued yet: IAt: %d, current epoch: %d", iat, cur)
	}
	if nbf := lt.Nbf; nbf > cur {
		return nil, fmt.Errorf("token is not valid yet: NBf: %d, current epoch: %d", nbf, cur)
	}

	return &token, nil
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

func (s *server) makePutResponse(body *protocontainer.PutResponse_Body, st *protostatus.Status, req *protocontainer.PutRequest) (*protocontainer.PutResponse, error) {
	resp := &protocontainer.PutResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

func (s *server) makeFailedPutResponse(err error, req *protocontainer.PutRequest) (*protocontainer.PutResponse, error) {
	return s.makePutResponse(nil, util.ToStatus(err), req)
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
		return s.makeFailedPutResponse(err, req)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeFailedPutResponse(errors.New("missing container signature"), req)
	}
	mCnr := reqBody.GetContainer()
	if mCnr == nil {
		return s.makeFailedPutResponse(errors.New("missing container"), req)
	}

	if mCnr.PlacementPolicy == nil {
		return s.makeFailedPutResponse(errors.New("missing storage policy"), req)
	}
	if err := verifyStoragePolicy(mCnr.PlacementPolicy); err != nil {
		return s.makeFailedPutResponse(fmt.Errorf("invalid storage policy: %w", err), req)
	}

	var cnr container.Container
	if err := cnr.FromProtoMessage(mCnr); err != nil {
		return s.makeFailedPutResponse(fmt.Errorf("invalid container: %w", err), req)
	}

	st, err := s.getVerifiedSessionToken(req)
	if err != nil {
		return s.makeFailedPutResponse(fmt.Errorf("verify session token: %w", err), req)
	}

	id, err := s.contract.Put(cnr, mSig.Key, mSig.Sign, st)
	if err != nil {
		return s.makeFailedPutResponse(err, req)
	}

	respBody := &protocontainer.PutResponse_Body{
		ContainerId: id.ProtoMessage(),
	}
	return s.makePutResponse(respBody, util.StatusOK, req)
}

func (s *server) makeDeleteResponse(err error, req *protocontainer.DeleteRequest) (*protocontainer.DeleteResponse, error) {
	resp := &protocontainer.DeleteResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

// Delete forwards container removal request to the underlying [Contract] for
// further processing. If session token is attached, it's verified.
func (s *server) Delete(_ context.Context, req *protocontainer.DeleteRequest) (*protocontainer.DeleteResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeDeleteResponse(err, req)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeDeleteResponse(errors.New("missing ID signature"), req)
	}
	mID := reqBody.GetContainerId()
	if mID == nil {
		return s.makeDeleteResponse(errors.New("missing ID"), req)
	}

	var id cid.ID
	if err := id.FromProtoMessage(mID); err != nil {
		return s.makeDeleteResponse(fmt.Errorf("invalid ID: %w", err), req)
	}

	st, err := s.getVerifiedSessionToken(req)
	if err != nil {
		return s.makeDeleteResponse(fmt.Errorf("verify session token: %w", err), req)
	}
	if st != nil {
		if err := s.checkSessionIssuer(id, st.Issuer()); err != nil {
			return s.makeDeleteResponse(fmt.Errorf("verify session issuer: %w", err), req)
		}
		if !st.AppliedTo(id) {
			return s.makeDeleteResponse(errors.New("session is not applied to requested container"), req)
		}
	}

	if err := s.contract.Delete(id, mSig.Key, mSig.Sign, st); err != nil {
		return s.makeDeleteResponse(err, req)
	}

	return s.makeDeleteResponse(util.StatusOKErr, req)
}

func (s *server) makeGetResponse(body *protocontainer.GetResponse_Body, st *protostatus.Status, req *protocontainer.GetRequest) (*protocontainer.GetResponse, error) {
	resp := &protocontainer.GetResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

func (s *server) makeFailedGetResponse(err error, req *protocontainer.GetRequest) (*protocontainer.GetResponse, error) {
	return s.makeGetResponse(nil, util.ToStatus(err), req)
}

// Get requests container from the underlying [Contract] and returns it in the
// response.
func (s *server) Get(_ context.Context, req *protocontainer.GetRequest) (*protocontainer.GetResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedGetResponse(err, req)
	}

	mID := req.GetBody().GetContainerId()
	if mID == nil {
		return s.makeFailedGetResponse(errors.New("missing ID"), req)
	}

	var id cid.ID
	if err := id.FromProtoMessage(mID); err != nil {
		return s.makeFailedGetResponse(fmt.Errorf("invalid ID: %w", err), req)
	}

	cnr, err := s.contract.Get(id)
	if err != nil {
		return s.makeFailedGetResponse(err, req)
	}

	body := &protocontainer.GetResponse_Body{
		Container: cnr.ProtoMessage(),
	}
	return s.makeGetResponse(body, nil, req)
}

func (s *server) makeListResponse(body *protocontainer.ListResponse_Body, st *protostatus.Status, req *protocontainer.ListRequest) (*protocontainer.ListResponse, error) {
	resp := &protocontainer.ListResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

func (s *server) makeFailedListResponse(err error, req *protocontainer.ListRequest) (*protocontainer.ListResponse, error) {
	return s.makeListResponse(nil, util.ToStatus(err), req)
}

// List lists user containers from the underlying [Contract] and returns their
// IDs in the response.
func (s *server) List(_ context.Context, req *protocontainer.ListRequest) (*protocontainer.ListResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedListResponse(err, req)
	}

	mID := req.GetBody().GetOwnerId()
	if mID == nil {
		return s.makeFailedListResponse(errors.New("missing user"), req)
	}

	var id user.ID
	if err := id.FromProtoMessage(mID); err != nil {
		return s.makeFailedListResponse(fmt.Errorf("invalid user: %w", err), req)
	}

	cs, err := s.contract.List(id)
	if err != nil {
		return s.makeFailedListResponse(err, req)
	}

	if len(cs) == 0 {
		return s.makeListResponse(nil, util.StatusOK, req)
	}

	body := &protocontainer.ListResponse_Body{
		ContainerIds: make([]*refs.ContainerID, len(cs)),
	}
	for i := range cs {
		body.ContainerIds[i] = cs[i].ProtoMessage()
	}
	return s.makeListResponse(body, util.StatusOK, req)
}

func (s *server) makeSetEACLResponse(err error, req *protocontainer.SetExtendedACLRequest) (*protocontainer.SetExtendedACLResponse, error) {
	resp := &protocontainer.SetExtendedACLResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

// SetExtendedACL forwards eACL setting request to the underlying [Contract]
// for further processing. If session token is attached, it's verified.
func (s *server) SetExtendedACL(_ context.Context, req *protocontainer.SetExtendedACLRequest) (*protocontainer.SetExtendedACLResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeSetEACLResponse(err, req)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeSetEACLResponse(errors.New("missing eACL signature"), req)
	}
	mEACL := reqBody.GetEacl()
	if mEACL == nil {
		return s.makeSetEACLResponse(errors.New("missing eACL"), req)
	}

	var eACL eacl.Table
	if err := eACL.FromProtoMessage(mEACL); err != nil {
		return s.makeSetEACLResponse(fmt.Errorf("invalid eACL: %w", err), req)
	}

	st, err := s.getVerifiedSessionToken(req)
	if err != nil {
		return s.makeSetEACLResponse(fmt.Errorf("verify session token: %w", err), req)
	}
	if st != nil {
		id := eACL.GetCID()
		if err := s.checkSessionIssuer(id, st.Issuer()); err != nil {
			return s.makeSetEACLResponse(fmt.Errorf("verify session issuer: %w", err), req)
		}
		if !st.AppliedTo(id) {
			return s.makeSetEACLResponse(errors.New("session is not applied to requested container"), req)
		}
	}

	if err := s.contract.PutEACL(eACL, mSig.Key, mSig.Sign, st); err != nil {
		return s.makeSetEACLResponse(err, req)
	}

	return s.makeSetEACLResponse(util.StatusOKErr, req)
}

func (s *server) makeGetEACLResponse(body *protocontainer.GetExtendedACLResponse_Body, st *protostatus.Status, req *protocontainer.GetExtendedACLRequest) (*protocontainer.GetExtendedACLResponse, error) {
	resp := &protocontainer.GetExtendedACLResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

func (s *server) makeFailedGetEACLResponse(err error, req *protocontainer.GetExtendedACLRequest) (*protocontainer.GetExtendedACLResponse, error) {
	return s.makeGetEACLResponse(nil, util.ToStatus(err), req)
}

// GetExtendedACL read eACL of the requested container from the underlying
// [Contract] and returns the result in the response.
func (s *server) GetExtendedACL(_ context.Context, req *protocontainer.GetExtendedACLRequest) (*protocontainer.GetExtendedACLResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeFailedGetEACLResponse(err, req)
	}

	mID := req.GetBody().GetContainerId()
	if mID == nil {
		return s.makeFailedGetEACLResponse(errors.New("missing ID"), req)
	}

	var id cid.ID
	if err := id.FromProtoMessage(mID); err != nil {
		return s.makeFailedGetEACLResponse(fmt.Errorf("invalid ID: %w", err), req)
	}

	eACL, err := s.contract.GetEACL(id)
	if err != nil {
		return s.makeFailedGetEACLResponse(err, req)
	}

	body := &protocontainer.GetExtendedACLResponse_Body{
		Eacl: eACL.ProtoMessage(),
	}
	return s.makeGetEACLResponse(body, util.StatusOK, req)
}
