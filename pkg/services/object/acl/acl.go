package acl

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	eaclV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl/v2"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	bearerSDK "github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// CheckerPrm groups parameters for Checker
// constructor.
type CheckerPrm struct {
	eaclSrc      container.EACLSource
	validator    *eaclSDK.Validator
	localStorage *engine.StorageEngine
	state        netmap.State
}

func (c *CheckerPrm) SetEACLSource(v container.EACLSource) *CheckerPrm {
	c.eaclSrc = v
	return c
}

func (c *CheckerPrm) SetValidator(v *eaclSDK.Validator) *CheckerPrm {
	c.validator = v
	return c
}

func (c *CheckerPrm) SetLocalStorage(v *engine.StorageEngine) *CheckerPrm {
	c.localStorage = v
	return c
}

func (c *CheckerPrm) SetNetmapState(v netmap.State) *CheckerPrm {
	c.state = v
	return c
}

// Checker implements v2.ACLChecker interfaces and provides
// ACL/eACL validation functionality.
type Checker struct {
	eaclSrc      container.EACLSource
	validator    *eaclSDK.Validator
	localStorage *engine.StorageEngine
	state        netmap.State
}

// Various EACL check errors.
var (
	errEACLDeniedByRule         = errors.New("denied by rule")
	errBearerExpired            = errors.New("bearer token has expired")
	errBearerInvalidSignature   = errors.New("bearer token has invalid signature")
	errBearerInvalidContainerID = errors.New("bearer token was created for another container")
	errBearerNotSignedByOwner   = errors.New("bearer token is not signed by the container owner")
	errBearerInvalidOwner       = errors.New("bearer token owner differs from the request sender")
)

// NewChecker creates Checker.
// Panics if at least one of the parameter is nil.
func NewChecker(prm *CheckerPrm) *Checker {
	panicOnNil := func(fieldName string, field any) {
		if field == nil {
			panic(fmt.Sprintf("incorrect field %s (%T): %v", fieldName, field, field))
		}
	}

	panicOnNil("EACLSource", prm.eaclSrc)
	panicOnNil("EACLValidator", prm.validator)
	panicOnNil("LocalStorageEngine", prm.localStorage)
	panicOnNil("NetmapState", prm.state)

	return &Checker{
		eaclSrc:      prm.eaclSrc,
		validator:    prm.validator,
		localStorage: prm.localStorage,
		state:        prm.state,
	}
}

// CheckBasicACL is a main check function for basic ACL.
func (c *Checker) CheckBasicACL(info v2.RequestInfo) bool {
	// check basic ACL permissions
	return info.BasicACL().IsOpAllowed(info.Operation(), info.RequestRole())
}

// StickyBitCheck validates owner field in the request if sticky bit is enabled.
func (c *Checker) StickyBitCheck(info v2.RequestInfo, owner user.ID) bool {
	// According to NeoFS specification sticky bit has no effect on system nodes
	// for correct intra-container work with objects (in particular, replication).
	if info.RequestRole() == acl.RoleContainer {
		return true
	}

	if !info.BasicACL().Sticky() {
		return true
	}

	if len(info.SenderKey()) == 0 {
		return false
	}

	return isOwnerFromKey(owner, info.SenderKey())
}

// CheckEACL is a main check function for extended ACL.
func (c *Checker) CheckEACL(msg any, reqInfo v2.RequestInfo) error {
	basicACL := reqInfo.BasicACL()
	if !basicACL.Extendable() {
		return nil
	}

	// if bearer token is not allowed, then ignore it
	if !basicACL.AllowedBearerRules(reqInfo.Operation()) {
		reqInfo.CleanBearer()
	}

	var table eaclSDK.Table
	cnr := reqInfo.ContainerID()

	bearerTok := reqInfo.Bearer()
	if bearerTok == nil {
		eaclInfo, err := c.eaclSrc.GetEACL(cnr)
		if err != nil {
			if client.IsErrEACLNotFound(err) {
				return nil
			}
			return err
		}

		table = *eaclInfo.Value
	} else {
		table = bearerTok.EACLTable()
	}

	// if bearer token is not present, isValidBearer returns true
	if err := isValidBearer(reqInfo, c.state); err != nil {
		return err
	}

	hdrSrcOpts := make([]eaclV2.Option, 0, 3)

	hdrSrcOpts = append(hdrSrcOpts,
		eaclV2.WithLocalObjectStorage(c.localStorage),
		eaclV2.WithCID(cnr),
		eaclV2.WithOID(reqInfo.ObjectID()),
	)

	if req, ok := msg.(eaclV2.Request); ok {
		hdrSrcOpts = append(hdrSrcOpts, eaclV2.WithServiceRequest(req))
	} else {
		hdrSrcOpts = append(hdrSrcOpts,
			eaclV2.WithServiceResponse(
				msg.(eaclV2.Response),
				reqInfo.Request().(eaclV2.Request),
			),
		)
	}

	hdrSrc, err := eaclV2.NewMessageHeaderSource(hdrSrcOpts...)
	if err != nil {
		return fmt.Errorf("can't parse headers: %w", err)
	}

	var eaclRole eaclSDK.Role
	switch op := reqInfo.RequestRole(); op {
	default:
		eaclRole = eaclSDK.Role(op)
	case acl.RoleOwner:
		eaclRole = eaclSDK.RoleUser
	case acl.RoleInnerRing, acl.RoleContainer:
		eaclRole = eaclSDK.RoleSystem
	case acl.RoleOthers:
		eaclRole = eaclSDK.RoleOthers
	}

	action, _ := c.validator.CalculateAction(new(eaclSDK.ValidationUnit).
		WithRole(eaclRole).
		WithOperation(eaclSDK.Operation(reqInfo.Operation())).
		WithContainerID(&cnr).
		WithSenderKey(reqInfo.SenderKey()).
		WithHeaderSource(hdrSrc).
		WithEACLTable(&table),
	)

	if action != eaclSDK.ActionAllow {
		return errEACLDeniedByRule
	}
	return nil
}

// isValidBearer checks whether bearer token was correctly signed by authorized
// entity. This method might be defined on whole ACL service because it will
// require fetching current epoch to check lifetime.
func isValidBearer(reqInfo v2.RequestInfo, st netmap.State) error {
	ownerCnr := reqInfo.ContainerOwner()

	token := reqInfo.Bearer()

	// 0. Check if bearer token is present in reqInfo.
	if token == nil {
		return nil
	}

	// 1. First check token lifetime. Simplest verification.
	if token.InvalidAt(st.CurrentEpoch()) {
		return errBearerExpired
	}

	// 2. Then check if bearer token is signed correctly.
	if !token.VerifySignature() {
		return errBearerInvalidSignature
	}

	// 3. Then check if container is either empty or equal to the container in the request.
	cnr, isSet := token.EACLTable().CID()
	if isSet && !cnr.Equals(reqInfo.ContainerID()) {
		return errBearerInvalidContainerID
	}

	// 4. Then check if container owner signed this token.
	if !bearerSDK.ResolveIssuer(*token).Equals(ownerCnr) {
		// TODO: #767 in this case we can issue all owner keys from neofs.id and check once again
		return errBearerNotSignedByOwner
	}

	// 5. Then check if request sender has rights to use this token.
	var usrSender user.ID
	err := user.IDFromKey(&usrSender, reqInfo.SenderKey())
	if err != nil {
		return fmt.Errorf("decode sender public key: %w", err)
	}

	if !token.AssertUser(usrSender) {
		// TODO: #767 in this case we can issue all owner keys from neofs.id and check once again
		return errBearerInvalidOwner
	}

	return nil
}

func isOwnerFromKey(id user.ID, key []byte) bool {
	if key == nil {
		return false
	}

	var id2 user.ID
	err := user.IDFromKey(&id2, key)
	if err != nil {
		return false
	}

	return id.Equals(id2)
}
