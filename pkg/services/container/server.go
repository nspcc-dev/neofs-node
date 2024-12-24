package container

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	apiacl "github.com/nspcc-dev/neofs-api-go/v2/acl"
	protoacl "github.com/nspcc-dev/neofs-api-go/v2/acl/grpc"
	apicontainer "github.com/nspcc-dev/neofs-api-go/v2/container"
	protocontainer "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	apirefs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	apisession "github.com/nspcc-dev/neofs-api-go/v2/session"
	protosession "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

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

type server struct {
	protocontainer.UnimplementedContainerServiceServer
	signer   *ecdsa.PrivateKey
	net      netmap.State
	contract Contract
}

// New provides protocontainer.ContainerServiceServer based on specified
// [Contract].
//
// All response messages are signed using specified signer and have current
// epoch in the meta header.
func New(s *ecdsa.PrivateKey, net netmap.State, c Contract) protocontainer.ContainerServiceServer {
	return &server{
		signer:   s,
		net:      net,
		contract: c,
	}
}

func (s *server) makeResponseMetaHeader(code uint32, msg string) *protosession.ResponseMetaHeader {
	v := version.Current()
	var v2 apirefs.Version
	v.WriteToV2(&v2)
	return &protosession.ResponseMetaHeader{
		Version: v2.ToGRPCMessage().(*refs.Version),
		Epoch:   s.net.CurrentEpoch(),
		Status:  &protostatus.Status{Code: code, Message: msg},
	}
}

// decodes the container session token from the request and checks its
// signature, lifetime and applicability to this operation as per request. If
// the token is invalid, non-zero code with error returns. If token is not
// attached to the request, nil and no error returns.
func (s *server) getVerifiedSessionToken(req interface {
	GetMetaHeader() *protosession.RequestMetaHeader
}) (*session.Container, uint32, error) {
	mh := req.GetMetaHeader()
	for omh := mh.GetOrigin(); omh != nil; omh = mh.GetOrigin() {
		mh = omh
	}
	m := mh.GetSessionToken()
	if m == nil {
		return nil, 0, nil
	}

	// TODO: use const codes on SDK upgrade
	var st2 apisession.Token
	if err := st2.FromGRPCMessage(mh); err != nil {
		panic(err)
	}
	var token session.Container
	if err := token.ReadFromV2(st2); err != nil {
		return nil, 1024, fmt.Errorf("decode: %w", err)
	}

	if !token.VerifySignature() {
		return nil, 1024, errors.New("invalid signature")
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
		return nil, 1024, fmt.Errorf("wrong operation: %s", verb)
	}

	cur := s.net.CurrentEpoch()
	lt := m.Body.Lifetime // must be checked by ReadFromV2, so NPE is OK here
	if exp := lt.Exp; exp < cur {
		return nil, 4097, errors.New("expired")
	}
	if iat := lt.Iat; iat > cur {
		return nil, 1024, fmt.Errorf("should not be issued yet: IAt: %d, current epoch: %d", iat, cur)
	}
	if nbf := lt.Nbf; nbf > cur {
		return nil, 1024, fmt.Errorf("not valid yet: NBf: %d, current epoch: %d", nbf, cur)
	}

	return &token, 0, nil
}

func (s *server) checkSessionIssuer(id cid.ID, token session.Container) (uint32, error) {
	cnr, err := s.contract.Get(id)
	if err != nil {
		return 1024, fmt.Errorf("get container by ID: %w", err)
	}

	var token2 apisession.Token // TODO: get signature directly from token on SDK upgrade
	token.WriteToV2(&token2)
	mSig := token2.GetSignature()

	var pub neofsecdsa.PublicKey
	if err := pub.Decode(mSig.GetKey()); err != nil {
		return 1024, fmt.Errorf("invalid public key in the session token signature: %w", err)
	}

	issuer := token.Issuer()
	if signer := user.NewFromECDSAPublicKey(ecdsa.PublicKey(pub)); signer != issuer {
		return 1024, errors.New("session token is signed not by its issuer")
	}

	if owner := cnr.Owner(); issuer != owner {
		return 1024, errors.New("session was not issued by the container owner")
	}

	return 0, nil
}

func (s *server) makePutResponse(body *protocontainer.PutResponse_Body, code uint32, msg string) (*protocontainer.PutResponse, error) {
	resp := &protocontainer.PutResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(code, msg),
	}
	return util.SignResponse(s.signer, resp, apicontainer.PutResponse{}), nil
}

func (s *server) makeFailedPutResponse(code uint32, err error) (*protocontainer.PutResponse, error) {
	return s.makePutResponse(nil, code, err.Error())
}

// Put forwards container creation request to the underlying [Contract] for
// further processing. If session token is attached, it's verified. Returns ID
// to check request status in the response.
func (s *server) Put(_ context.Context, req *protocontainer.PutRequest) (*protocontainer.PutResponse, error) {
	putReq := new(apicontainer.PutRequest)
	if err := putReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	// TODO: use const codes on SDK upgrade
	if err := signature.VerifyServiceMessage(putReq); err != nil {
		return s.makeFailedPutResponse(1026, err)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeFailedPutResponse(1024, errors.New("missing container signature"))
	}
	mCnr := reqBody.GetContainer()
	if mCnr == nil {
		return s.makeFailedPutResponse(1024, errors.New("missing container"))
	}

	var cnr container.Container
	var cnr2 apicontainer.Container
	if err := cnr2.FromGRPCMessage(mCnr); err != nil {
		panic(err)
	}
	if err := cnr.ReadFromV2(cnr2); err != nil {
		return s.makeFailedPutResponse(1024, fmt.Errorf("invalid container: %w", err))
	}

	st, code, err := s.getVerifiedSessionToken(req)
	if code != 0 {
		return s.makeFailedPutResponse(code, fmt.Errorf("verify session token: %w", err))
	}

	id, err := s.contract.Put(cnr, mSig.Key, mSig.Sign, st)
	if err != nil {
		return s.makeFailedPutResponse(1024, err)
	}

	var id2 apirefs.ContainerID
	id.WriteToV2(&id2)
	respBody := &protocontainer.PutResponse_Body{
		ContainerId: id2.ToGRPCMessage().(*refs.ContainerID),
	}
	return s.makePutResponse(respBody, 0, "")
}

func (s *server) makeDeleteResponse(code uint32, err error) (*protocontainer.DeleteResponse, error) {
	var msg string
	if err != nil {
		msg = err.Error()
	}
	resp := &protocontainer.DeleteResponse{
		MetaHeader: s.makeResponseMetaHeader(code, msg),
	}
	return util.SignResponse(s.signer, resp, apicontainer.DeleteResponse{}), nil
}

// Delete forwards container removal request to the underlying [Contract] for
// further processing. If session token is attached, it's verified.
func (s *server) Delete(_ context.Context, req *protocontainer.DeleteRequest) (*protocontainer.DeleteResponse, error) {
	delReq := new(apicontainer.DeleteRequest)
	if err := delReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	// TODO: use const codes on SDK upgrade
	if err := signature.VerifyServiceMessage(delReq); err != nil {
		return s.makeDeleteResponse(1026, err)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeDeleteResponse(1024, errors.New("missing ID signature"))
	}
	mID := reqBody.GetContainerId()
	if mID == nil {
		return s.makeDeleteResponse(1024, errors.New("missing ID"))
	}

	var id2 apirefs.ContainerID
	if err := id2.FromGRPCMessage(mID); err != nil {
		panic(err)
	}
	var id cid.ID
	if err := id.ReadFromV2(id2); err != nil {
		return s.makeDeleteResponse(1024, fmt.Errorf("invalid ID: %w", err))
	}

	st, code, err := s.getVerifiedSessionToken(req)
	if code != 0 {
		return s.makeDeleteResponse(code, fmt.Errorf("verify session token: %w", err))
	}
	if st != nil {
		if code, err := s.checkSessionIssuer(id, *st); code != 0 {
			return s.makeDeleteResponse(code, fmt.Errorf("verify session issuer: %w", err))
		}
		if !st.AppliedTo(id) {
			return s.makeDeleteResponse(1024, errors.New("session is not applied to requested container"))
		}
	}

	if err := s.contract.Delete(id, mSig.Key, mSig.Sign, st); err != nil {
		return s.makeDeleteResponse(1024, err)
	}

	return s.makeDeleteResponse(0, nil)
}

func (s *server) makeGetResponse(body *protocontainer.GetResponse_Body, code uint32, msg string) (*protocontainer.GetResponse, error) {
	resp := &protocontainer.GetResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(code, msg),
	}
	return util.SignResponse(s.signer, resp, apicontainer.GetResponse{}), nil
}

func (s *server) makeFailedGetResponse(code uint32, err error) (*protocontainer.GetResponse, error) {
	return s.makeGetResponse(nil, code, err.Error())
}

// Get requests container from the underlying [Contract] and returns it in the
// response.
func (s *server) Get(_ context.Context, req *protocontainer.GetRequest) (*protocontainer.GetResponse, error) {
	getReq := new(apicontainer.GetRequest)
	if err := getReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	// TODO: use const codes on SDK upgrade
	if err := signature.VerifyServiceMessage(getReq); err != nil {
		return s.makeFailedGetResponse(1026, err)
	}

	mID := req.GetBody().GetContainerId()
	if mID == nil {
		return s.makeFailedGetResponse(1024, errors.New("missing ID"))
	}

	var id2 apirefs.ContainerID
	if err := id2.FromGRPCMessage(mID); err != nil {
		panic(err)
	}
	var id cid.ID
	if err := id.ReadFromV2(id2); err != nil {
		return s.makeFailedGetResponse(1024, fmt.Errorf("invalid ID: %w", err))
	}

	cnr, err := s.contract.Get(id)
	if err != nil {
		if errors.Is(err, apistatus.ErrContainerNotFound) {
			return s.makeFailedGetResponse(3072, err)
		}
		return s.makeFailedGetResponse(1024, err)
	}

	var cnr2 apicontainer.Container
	cnr.WriteToV2(&cnr2)
	body := &protocontainer.GetResponse_Body{
		Container: cnr2.ToGRPCMessage().(*protocontainer.Container),
	}
	return s.makeGetResponse(body, 0, "")
}

func (s *server) makeListResponse(body *protocontainer.ListResponse_Body, code uint32, msg string) (*protocontainer.ListResponse, error) {
	resp := &protocontainer.ListResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(code, msg),
	}
	return util.SignResponse(s.signer, resp, apicontainer.ListResponse{}), nil
}

func (s *server) makeFailedListResponse(code uint32, err error) (*protocontainer.ListResponse, error) {
	return s.makeListResponse(nil, code, err.Error())
}

// List lists user containers from the underlying [Contract] and returns their
// IDs in the response.
func (s *server) List(_ context.Context, req *protocontainer.ListRequest) (*protocontainer.ListResponse, error) {
	listReq := new(apicontainer.ListRequest)
	if err := listReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	// TODO: use const codes on SDK upgrade
	if err := signature.VerifyServiceMessage(listReq); err != nil {
		return s.makeFailedListResponse(1026, err)
	}

	mID := req.GetBody().GetOwnerId()
	if mID == nil {
		return s.makeFailedListResponse(1024, errors.New("missing user"))
	}

	var id2 apirefs.OwnerID
	if err := id2.FromGRPCMessage(mID); err != nil {
		panic(err)
	}
	var id user.ID
	if err := id.ReadFromV2(id2); err != nil {
		return s.makeFailedListResponse(1024, fmt.Errorf("invalid user: %w", err))
	}

	cs, err := s.contract.List(id)
	if err != nil {
		return s.makeFailedListResponse(1024, err)
	}

	if len(cs) == 0 {
		return s.makeListResponse(nil, 0, "")
	}

	cs2 := make([]apirefs.ContainerID, len(cs))
	for i := range cs {
		cs[i].WriteToV2(&cs2[i])
	}
	body := &protocontainer.ListResponse_Body{
		ContainerIds: make([]*refs.ContainerID, len(cs)),
	}
	for i := range cs {
		body.ContainerIds[i] = cs2[i].ToGRPCMessage().(*refs.ContainerID)
	}
	return s.makeListResponse(body, 0, "")
}

func (s *server) makeSetEACLResponse(code uint32, err error) (*protocontainer.SetExtendedACLResponse, error) {
	var msg string
	if err != nil {
		msg = err.Error()
	}
	resp := &protocontainer.SetExtendedACLResponse{
		MetaHeader: s.makeResponseMetaHeader(code, msg),
	}
	return util.SignResponse(s.signer, resp, apicontainer.SetExtendedACLResponse{}), nil
}

// SetExtendedACL forwards eACL setting request to the underlying [Contract]
// for further processing. If session token is attached, it's verified.
func (s *server) SetExtendedACL(_ context.Context, req *protocontainer.SetExtendedACLRequest) (*protocontainer.SetExtendedACLResponse, error) {
	setEACLReq := new(apicontainer.SetExtendedACLRequest)
	if err := setEACLReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	// TODO: use const codes on SDK upgrade
	if err := signature.VerifyServiceMessage(setEACLReq); err != nil {
		return s.makeSetEACLResponse(1026, err)
	}

	reqBody := req.GetBody()
	mSig := reqBody.GetSignature()
	if mSig == nil {
		return s.makeSetEACLResponse(1024, errors.New("missing eACL signature"))
	}
	mEACL := reqBody.GetEacl()
	if mEACL == nil {
		return s.makeSetEACLResponse(1024, errors.New("missing eACL"))
	}

	var eACL2 apiacl.Table
	if err := eACL2.FromGRPCMessage(mEACL); err != nil {
		panic(err)
	}
	var eACL eacl.Table
	if err := eACL.ReadFromV2(eACL2); err != nil {
		return s.makeSetEACLResponse(1024, fmt.Errorf("invalid eACL: %w", err))
	}

	st, code, err := s.getVerifiedSessionToken(req)
	if code != 0 {
		return s.makeSetEACLResponse(code, fmt.Errorf("verify session token: %w", err))
	}
	if st != nil {
		id := eACL.GetCID()
		if code, err := s.checkSessionIssuer(id, *st); code != 0 {
			return s.makeSetEACLResponse(code, fmt.Errorf("verify session issuer: %w", err))
		}
		if !st.AppliedTo(id) {
			return s.makeSetEACLResponse(1024, errors.New("session is not applied to requested container"))
		}
	}

	if err := s.contract.PutEACL(eACL, mSig.Key, mSig.Sign, st); err != nil {
		return s.makeSetEACLResponse(1024, err)
	}

	return s.makeSetEACLResponse(0, nil)
}

func (s *server) makeGetEACLResponse(body *protocontainer.GetExtendedACLResponse_Body, code uint32, msg string) (*protocontainer.GetExtendedACLResponse, error) {
	resp := &protocontainer.GetExtendedACLResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(code, msg),
	}
	return util.SignResponse(s.signer, resp, apicontainer.GetExtendedACLResponse{}), nil
}

func (s *server) makeFailedGetEACLResponse(code uint32, err error) (*protocontainer.GetExtendedACLResponse, error) {
	return s.makeGetEACLResponse(nil, code, err.Error())
}

// GetExtendedACL read eACL of the requested container from the underlying
// [Contract] and returns the result in the response.
func (s *server) GetExtendedACL(_ context.Context, req *protocontainer.GetExtendedACLRequest) (*protocontainer.GetExtendedACLResponse, error) {
	getEACLReq := new(apicontainer.GetExtendedACLRequest)
	if err := getEACLReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	// TODO: use const codes on SDK upgrade
	if err := signature.VerifyServiceMessage(getEACLReq); err != nil {
		return s.makeFailedGetEACLResponse(1026, err)
	}

	mID := req.GetBody().GetContainerId()
	if mID == nil {
		return s.makeFailedGetEACLResponse(1024, errors.New("missing ID"))
	}

	var id2 apirefs.ContainerID
	if err := id2.FromGRPCMessage(mID); err != nil {
		panic(err)
	}
	var id cid.ID
	if err := id.ReadFromV2(id2); err != nil {
		return s.makeFailedGetEACLResponse(1024, fmt.Errorf("invalid ID: %w", err))
	}

	eACL, err := s.contract.GetEACL(id)
	if err != nil {
		if errors.Is(err, apistatus.ErrEACLNotFound) {
			return s.makeFailedGetEACLResponse(3073, err)
		}
		return s.makeFailedGetEACLResponse(1024, err)
	}

	body := &protocontainer.GetExtendedACLResponse_Body{
		Eacl: eACL.ToV2().ToGRPCMessage().(*protoacl.EACLTable),
	}
	return s.makeGetEACLResponse(body, 0, "")
}
