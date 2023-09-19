package acl

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// ContainerInfo groups information about NeoFS container processed by
// [Checker].
type ContainerInfo struct {
	BasicACL acl.Basic
	Owner    user.ID
}

// NeoFS provides access to the NeoFS network used by [Checker] to process.
type NeoFS interface {
	// GetContainerInfo reads information about the NeoFS container by its ID.
	// Returns [apistatus.ErrContainerNotFound] if the container is missing.
	GetContainerInfo(cid.ID) (ContainerInfo, error)

	// GetExtendedACL reads extended ACL of the referenced NeoFS container. Called
	// only for containers with extendable ACL. Returns [apistatus.ErrEACLNotFound]
	// if the eACL is missing.
	GetExtendedACL(cid.ID) (eacl.Table, error)

	// CurrentEpoch reads number of the current NeoFS epoch.
	CurrentEpoch() (uint64, error)

	// ResolveUserByPublicKey resolves NeoFS user by the given binary public key.
	ResolveUserByPublicKey(bPublicKey []byte) (user.ID, error)

	// IsUserPublicKey checks whether specified binary-encoded public key
	// belongs to the specified NeoFS user.
	IsUserPublicKey(usr user.ID, bPublicKey []byte) (bool, error)

	// IsInnerRingPublicKey checks whether specified binary-encoded public key
	// belongs to any NeoFS Inner Ring member.
	IsInnerRingPublicKey(bPublicKey []byte) (bool, error)

	// IsContainerNodePublicKey check whether specified binary-encoded public key
	// belongs to any storage node selected to the referenced NeoFS container.
	IsContainerNodePublicKey(cnr cid.ID, bPublicKey []byte) (bool, error)
}

// Node represents storage node within which [Checker] works.
type Node interface {
	NeoFS

	// ReadLocalObjectHeaders reads all headers of the referenced object stored on
	// the Node. Returns any error encountered that prevented object to be read.
	ReadLocalObjectHeaders(cnr cid.ID, id oid.ID) (object.Object, error)
}

// Checker manages access to the NeoFS object(s) requested by external clients
// via upstream storage node.
type Checker struct {
	node Node
}

// NewChecker constructs Checker for the specified Node instance.
func NewChecker(node Node) *Checker {
	if node == nil {
		panic("nil node arg")
	}

	return &Checker{
		node: node,
	}
}

// ContainerRequest groups information about client request related to the
// particular NeoFS container to be processed by [Checker].
type ContainerRequest struct {
	cnr cid.ID

	payload ContainerOpPayload

	bClientPubKey []byte

	reqHeaders RequestHeaders

	bearerToken *bearer.Token

	sessionToken *session.Object
}

// ContainerOpPayload groups information about particular operation within the
// upstream NeoFS container.
type ContainerOpPayload struct {
	op acl.Op

	objID   *oid.ID
	objHdrs *object.Object
}

// PutRequest constructs OpPayload for the request to store object with
// specified header into the upstream container.
func PutRequest(objHdr object.Object) ContainerOpPayload {
	var objID *oid.ID

	if id, set := objHdr.ID(); set {
		objID = &id
	}

	return ContainerOpPayload{
		op:      acl.OpObjectPut,
		objID:   objID,
		objHdrs: &objHdr,
	}
}

// DeleteRequest constructs OpPayload for the request to delete object with
// specified ID from the upstream container.
func DeleteRequest(obj oid.ID) ContainerOpPayload {
	return ContainerOpPayload{
		op:    acl.OpObjectDelete,
		objID: &obj,
	}
}

// SearchRequest constructs OpPayload for the request to search for object in
// the upstream container.
func SearchRequest() ContainerOpPayload {
	return ContainerOpPayload{
		op: acl.OpObjectSearch,
	}
}

// GetRequest constructs OpPayload for the request to get object with the
// specified ID from the upstream container.
func GetRequest(obj oid.ID) ContainerOpPayload {
	return ContainerOpPayload{
		op:    acl.OpObjectGet,
		objID: &obj,
	}
}

// GetResponse constructs OpPayload for the response carrying given header of
// the requested object with the specified ID from the upstream container.
func GetResponse(obj oid.ID, hdr object.Object) ContainerOpPayload {
	return ContainerOpPayload{
		op:      acl.OpObjectGet,
		objID:   &obj,
		objHdrs: &hdr,
	}
}

// HeadRequest constructs OpPayload for the request to get header of the object
// with the specified ID from the upstream container.
func HeadRequest(obj oid.ID) ContainerOpPayload {
	return ContainerOpPayload{
		op:    acl.OpObjectHead,
		objID: &obj,
	}
}

// HeadResponse constructs OpPayload for the response to the request to get
// header of the object with the specified ID from the upstream container.
func HeadResponse(obj oid.ID, hdr object.Object) ContainerOpPayload {
	return ContainerOpPayload{
		op:      acl.OpObjectHead,
		objID:   &obj,
		objHdrs: &hdr,
	}
}

// PayloadRangeRequest constructs OpPayload for the request to get payload range
// of the object with the specified ID from the upstream container.
func PayloadRangeRequest(obj oid.ID) ContainerOpPayload {
	return ContainerOpPayload{
		op:    acl.OpObjectRange,
		objID: &obj,
	}
}

// HashPayloadRangeRequest constructs OpPayload for the request to hash payload
// range of the object with the specified ID from the upstream container.
func HashPayloadRangeRequest(obj oid.ID) ContainerOpPayload {
	return ContainerOpPayload{
		op:    acl.OpObjectHash,
		objID: &obj,
	}
}

// NewContainerRequest constructs new ContainerRequest instance.
//
// Binary public key of the client MUST be non-empty. ContainerOpPayload MUST be
// initialized by one of the constructors. RequestHeaders headers MUST be
// non-nil.
func NewContainerRequest(cnr cid.ID, clientPubKey []byte, payload ContainerOpPayload, reqHeaders RequestHeaders) ContainerRequest {
	switch {
	case payload == ContainerOpPayload{}:
		panic("uninitialized op payload")
	case len(clientPubKey) == 0:
		panic("empty client public key arg")
	case reqHeaders == nil:
		panic("nil request headers")
	}

	return ContainerRequest{
		cnr:           cnr,
		payload:       payload,
		bClientPubKey: clientPubKey,
		reqHeaders:    reqHeaders,
	}
}

// SetSessionToken sets unchecked session token attached to the request if any.
func (x *ContainerRequest) SetSessionToken(sessionToken session.Object) {
	x.sessionToken = &sessionToken
}

// SetBearerToken sets unchecked bearer token attached to the request if any.
func (x *ContainerRequest) SetBearerToken(bearerToken bearer.Token) {
	x.bearerToken = &bearerToken
}

// CheckAccess checks whether ContainerRequest is compliant with the access
// policy of the container. Returns:
//   - nil if operation is allowed
//   - [AccessDeniedError] if operation is prohibited
//   - [InvalidRequestError] if the request is invalid
//   - [apistatus.ErrSessionTokenExpired] if specified session token expired
//   - [apistatus.ErrContainerNotFound] if requested container is missing in the NeoFS
//   - [ErrNotEnoughData] if there are not enough inputs to determine the exact
//     result. Currently, can only happen with [HeadRequest] and [GetRequest]. For
//     these cases it is necessary to check the response
func (c *Checker) CheckAccess(req ContainerRequest) error {
	var curEpoch uint64
	var err error

	if req.sessionToken != nil {
		curEpoch, err = c.node.CurrentEpoch()
		if err != nil {
			return fmt.Errorf("get current NeoFS epoch: %w", err)
		}

		req.bClientPubKey, err = c.verifySessionTokenAndGetIssuerPublicKey(*req.sessionToken, curEpoch, req.bClientPubKey, req.payload.op, req.cnr, req.payload.objID)
		if err != nil {
			return err
		}
	}

	cnrInfo, err := c.node.GetContainerInfo(req.cnr)
	if err != nil {
		return fmt.Errorf("read requested container: %w", err)
	}

	clientUsr, err := c.node.ResolveUserByPublicKey(req.bClientPubKey)
	if err != nil {
		return fmt.Errorf("resolve client user by its public key: %w", err)
	}

	clientRole, err := c.resolveClientRole(req.bClientPubKey, clientUsr, req.cnr, cnrInfo.Owner)
	if err != nil {
		return fmt.Errorf("resolve client role: %w", err)
	}

	if !cnrInfo.BasicACL.IsOpAllowed(req.payload.op, clientRole) {
		return newAccessDeniedError(newBasicRuleError(newForbiddenClientRoleError(clientRole)))
	}

	// According to NeoFS specification, sticky rule has no effect on system nodes
	// for correct intra-container work with objects (in particular, replication).
	if clientRole != acl.RoleContainer && req.payload.op == acl.OpObjectPut && cnrInfo.BasicACL.Sticky() {
		if req.payload.objHdrs == nil {
			return newAccessDeniedError(newBasicRuleError(newStickyAccessRuleError(errMissingObjectHeader)))
		}

		objOwner := req.payload.objHdrs.OwnerID()
		if objOwner == nil {
			return newAccessDeniedError(newBasicRuleError(newStickyAccessRuleError(errMissingObjectOwner)))
		} else if !objOwner.Equals(cnrInfo.Owner) {
			return newAccessDeniedError(newBasicRuleError(newStickyAccessRuleError(errObjectOwnerAuth)))
		}
	}

	if cnrInfo.BasicACL.Extendable() {
		// if bearer token is not allowed, it is just ignored
		if req.bearerToken != nil && cnrInfo.BasicACL.AllowedBearerRules(req.payload.op) {
			if curEpoch == 0 {
				curEpoch, err = c.node.CurrentEpoch()
				if err != nil {
					return fmt.Errorf("get current NeoFS epoch: %w", err)
				}
			}

			eACL, err := c.getVerifiedBearerEACL(*req.bearerToken, curEpoch, req.cnr, cnrInfo.Owner, clientUsr)
			if err != nil {
				return fmt.Errorf("eACL from bearer token: %w", err)
			}

			return c.checkEACLAccess(eACL, req.cnr, req.payload, req.reqHeaders, req.bClientPubKey, clientRole)
		} else {
			eACL, err := c.node.GetExtendedACL(req.cnr)
			switch {
			default:
				return fmt.Errorf("read eACL of the requested container from NeoFS: %w", err)
			case err == nil:
				return c.checkEACLAccess(eACL, req.cnr, req.payload, req.reqHeaders, req.bClientPubKey, clientRole)
			case errors.Is(err, apistatus.ErrEACLNotFound):
				// unset rules have no effect
			}
		}
	}

	return nil
}

func (c *Checker) resolveClientRole(bClientPubKey []byte, clientUsr user.ID, cnrID cid.ID, cnrOwner user.ID) (acl.Role, error) {
	if clientUsr.Equals(cnrOwner) {
		return acl.RoleOwner, nil
	}

	clientIsInnerRing, err := c.node.IsInnerRingPublicKey(bClientPubKey)
	if err != nil {
		return 0, fmt.Errorf("check Inner Ring public keys: %w", err)
	}

	if clientIsInnerRing {
		return acl.RoleInnerRing, nil
	}

	clientIsStorageNode, err := c.node.IsContainerNodePublicKey(cnrID, bClientPubKey)
	if err != nil {
		return 0, fmt.Errorf("check container nodes' public keys: %w", err)
	}

	if clientIsStorageNode {
		return acl.RoleContainer, nil
	}

	return acl.RoleOthers, nil
}

func (c *Checker) verifySessionTokenAndGetIssuerPublicKey(
	sessionToken session.Object,
	curEpoch uint64,
	bClientPubKey []byte,
	op acl.Op,
	cnr cid.ID,
	obj *oid.ID,
) ([]byte, error) {
	if !sessionToken.VerifySignature() {
		return nil, newInvalidRequestError(newInvalidSessionTokenError(errInvalidSignature))
	}

	bIssuerPubKey := sessionToken.IssuerPublicKeyBytes()
	issuerUsr := sessionToken.Issuer()

	isIssuerKey, err := c.node.IsUserPublicKey(issuerUsr, bIssuerPubKey)
	if err != nil {
		return nil, fmt.Errorf("check session issuer key binding: %w", err)
	}

	if !isIssuerKey {
		return nil, newInvalidRequestError(newInvalidSessionTokenError(errSessionIssuerAuth))
	}

	if op != acl.OpObjectPut && op != acl.OpObjectDelete {
		// PUT and DELETE are specific: sessions in such requests may be dynamic, i.e.
		// opened using SessionService.Create and subject for the dynamically created
		// session private key located on the local node. Corresponding checks are
		// expected to be done at the other app layer.
		var clientPubKey neofsecdsa.PublicKey

		err = clientPubKey.Decode(bClientPubKey)
		if err != nil {
			return nil, newInvalidRequestError(newInvalidSessionTokenError(fmt.Errorf("decode public key from the signature: %w", err)))
		}

		if !sessionToken.AssertAuthKey(&clientPubKey) {
			return nil, newInvalidRequestError(newInvalidSessionTokenError(errSessionClientAuth))
		}
	}

	if !sessionToken.AssertContainer(cnr) {
		return nil, newInvalidRequestError(newInvalidSessionTokenError(errSessionContainerMismatch))
	}

	if obj != nil && !sessionToken.AssertObject(*obj) {
		// if session relates to object's removal, we don't check relation of the
		// tombstone to the session here since user can't predict tomb's ID.
		if op != acl.OpObjectPut || !sessionToken.AssertVerb(session.VerbObjectDelete) {
			return nil, newInvalidRequestError(newInvalidSessionTokenError(errSessionObjectMismatch))
		}
	}

	verbMatches := false

	switch op {
	case acl.OpObjectPut:
		verbMatches = sessionToken.AssertVerb(session.VerbObjectPut, session.VerbObjectDelete)
	case acl.OpObjectDelete:
		verbMatches = sessionToken.AssertVerb(session.VerbObjectDelete)
	case acl.OpObjectGet:
		verbMatches = sessionToken.AssertVerb(session.VerbObjectGet)
	case acl.OpObjectHead:
		verbMatches = sessionToken.AssertVerb(
			session.VerbObjectHead,
			session.VerbObjectGet,
			session.VerbObjectDelete,
			session.VerbObjectRange,
			session.VerbObjectRangeHash)
	case acl.OpObjectSearch:
		verbMatches = sessionToken.AssertVerb(session.VerbObjectSearch, session.VerbObjectDelete)
	case acl.OpObjectRange:
		verbMatches = sessionToken.AssertVerb(session.VerbObjectRange, session.VerbObjectRangeHash)
	case acl.OpObjectHash:
		verbMatches = sessionToken.AssertVerb(session.VerbObjectRangeHash)
	}

	if !verbMatches {
		return nil, newInvalidRequestError(newInvalidSessionTokenError(errSessionOpMismatch))
	}

	if sessionToken.ExpiredAt(curEpoch) {
		return nil, newInvalidRequestError(newInvalidSessionTokenError(apistatus.ErrSessionTokenExpired))
	}

	if sessionToken.InvalidAt(curEpoch) {
		return nil, newInvalidRequestError(newInvalidSessionTokenError(newInvalidAtEpochError(curEpoch)))
	}

	return bIssuerPubKey, nil
}

func (c *Checker) getVerifiedBearerEACL(bearerToken bearer.Token, curEpoch uint64, cnrID cid.ID, cnrOwner, clientUsr user.ID) (eacl.Table, error) {
	eACL := bearerToken.EACLTable()

	if !bearerToken.VerifySignature() {
		return eACL, newInvalidBearerTokenError(errInvalidSignature)
	}

	bIssuerKey := bearerToken.SigningKeyBytes()

	issuerUsr, err := c.node.ResolveUserByPublicKey(bIssuerKey)
	if err != nil {
		return eACL, fmt.Errorf("resolve bearer user by public key from the token signature: %w", err)
	}

	if !issuerUsr.Equals(cnrOwner) {
		return eACL, newInvalidBearerTokenError(errBearerIssuerAuth)
	}

	if !bearerToken.AssertUser(clientUsr) {
		return eACL, newInvalidBearerTokenError(errBearerClientAuth)
	}

	cnrInToken, isSet := eACL.CID()
	if isSet && !cnrInToken.Equals(cnrID) {
		return eACL, newInvalidBearerTokenError(errBearerContainerMismatch)
	}

	if bearerToken.InvalidAt(curEpoch) {
		return eACL, newInvalidBearerTokenError(newInvalidAtEpochError(curEpoch))
	}

	return eACL, nil
}

func (c *Checker) checkEACLAccess(eACL eacl.Table, cnr cid.ID, payload ContainerOpPayload, reqHeaders RequestHeaders, bClientPubKey []byte, clientRole acl.Role) error {
	objHdrs := newObjectHeadersContext(c.node, cnr, payload)

	records := eACL.Records()
	for i := range records {
		var denyOnMatch bool

		switch action := records[i].Action(); action {
		default:
			return fmt.Errorf("process record #%d: unsupported action #%d", i, action)
		case eacl.ActionDeny:
			denyOnMatch = true
		case eacl.ActionAllow:
			denyOnMatch = false
		}

		var op acl.Op

		switch records[i].Operation() {
		default:
			return fmt.Errorf("process record #%d: unsupported operation #%d", i, op)
		case eacl.OperationGet:
			op = acl.OpObjectGet
		case eacl.OperationHead:
			op = acl.OpObjectHead
		case eacl.OperationPut:
			op = acl.OpObjectPut
		case eacl.OperationDelete:
			op = acl.OpObjectDelete
		case eacl.OperationSearch:
			op = acl.OpObjectSearch
		case eacl.OperationRange:
			op = acl.OpObjectRange
		case eacl.OperationRangeHash:
			op = acl.OpObjectHash
		}

		if op != payload.op {
			continue
		}

		matchesClient, err := ruleMatchesClient(records[i], clientRole, bClientPubKey)
		if err != nil {
			return fmt.Errorf("process record #%d: %w", i, err)
		}

		if !matchesClient {
			continue
		}

		matchesResource, err := ruleMatchesResource(records[i], &objHdrs, reqHeaders)
		if err != nil {
			return fmt.Errorf("process record #%d: %w", i, err)
		}

		if matchesResource {
			if denyOnMatch {
				return newAccessDeniedError(newExtendedRuleError(i))
			}

			return nil
		}
	}

	return nil
}

// checks whether given access rule matches client with specified role and
// binary public key.
func ruleMatchesClient(rec eacl.Record, clientRole acl.Role, bClientPubKey []byte) (bool, error) {
	targets := rec.Targets()
	for i := range targets {
		if targetKeys := targets[i].BinaryKeys(); len(targetKeys) != 0 {
			for i := range targetKeys {
				if bytes.Equal(targetKeys[i], bClientPubKey) {
					return true, nil
				}
			}
			continue
		}

		targetRole := targets[i].Role()
		switch targetRole {
		default:
			return false, fmt.Errorf("process target #%d: unsupported subject role #%d", i, targetRole)
		case eacl.RoleUnknown, eacl.RoleSystem:
			// system role access modifications have been deprecated
		case eacl.RoleUser:
			if clientRole == acl.RoleOwner {
				return true, nil
			}
		case eacl.RoleOthers:
			if clientRole == acl.RoleOthers {
				return true, nil
			}
		}
	}

	return false, nil
}

// checks whether given access rule matches the resource represented by
// specified headers. Returns [ErrNotEnoughData] if object headers are
// unavailable at the moment but may be so in other setting (e.g. GET and HEAD
// have no object header in request but have one in response).
func ruleMatchesResource(rec eacl.Record, objHeaders *objectHeadersContext, reqHeaders RequestHeaders) (bool, error) {
	filters := rec.Filters()
	matchedFilters := 0
	for i := range filters {
		var hdrValue string

		switch hdrType := filters[i].From(); hdrType {
		default:
			return false, fmt.Errorf("process filter #%d: unsupported type of headers #%d", i, hdrType)
		case eacl.HeaderFromService:
			// ignored by storage nodes
			continue
		case eacl.HeaderFromRequest:
			hdrValue = reqHeaders.GetRequestHeaderByKey(filters[i].Key())
			if hdrValue == "" {
				continue
			}
		case eacl.HeaderFromObject:
			var err error
			hdrValue, err = objHeaders.getObjectHeaderByKey(filters[i].Key())
			if err != nil {
				switch {
				default:
					return false, err
				case errors.Is(err, errHeaderNotAvailable):
					if objHeaders.op == acl.OpObjectGet || objHeaders.op == acl.OpObjectHead {
						return false, ErrNotEnoughData
					}

					continue
				case errors.Is(err, errHeaderNotFound):
					// process empty object header value then
				}
			}
		}

		switch matcher := filters[i].Matcher(); matcher {
		default:
			return false, fmt.Errorf("process filter #%d: unsupported matcher #%d", i, matcher)
		case eacl.MatchStringEqual:
			if hdrValue == filters[i].Value() {
				matchedFilters++
			}
		case eacl.MatchStringNotEqual:
			if hdrValue != filters[i].Value() {
				matchedFilters++
			}
		}
	}

	return matchedFilters == len(filters), nil
}
