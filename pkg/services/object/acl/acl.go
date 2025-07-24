package acl

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	eaclV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl/v2"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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
	headerSource eaclV2.HeaderSource
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

func (c *CheckerPrm) SetHeaderSource(hs eaclV2.HeaderSource) *CheckerPrm {
	c.headerSource = hs
	return c
}

// Checker implements v2.ACLChecker interfaces and provides
// ACL/eACL validation functionality.
type Checker struct {
	eaclSrc      container.EACLSource
	validator    *eaclSDK.Validator
	localStorage *engine.StorageEngine
	headerSource eaclV2.HeaderSource
}

// Various EACL check errors.
var (
	errEACLDeniedByRule = errors.New("denied by rule")
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
	panicOnNil("HeaderSource", prm.headerSource)

	return &Checker{
		eaclSrc:      prm.eaclSrc,
		validator:    prm.validator,
		localStorage: prm.localStorage,
		headerSource: prm.headerSource,
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

	if eaclRole == eaclSDK.RoleSystem {
		return nil // Controlled by BasicACL, EACL can not contain any rules for system role since 0.38.0.
	}

	// if bearer token is not allowed, then ignore it
	if !basicACL.AllowedBearerRules(reqInfo.Operation()) {
		reqInfo.CleanBearer()
	}

	var table eaclSDK.Table
	cnr := reqInfo.ContainerID()

	bearerTok := reqInfo.Bearer()
	if bearerTok == nil {
		var err error
		table, err = c.eaclSrc.GetEACL(cnr)
		if err != nil {
			if errors.Is(err, apistatus.ErrEACLNotFound) {
				return nil
			}
			return err
		}
	} else {
		table = bearerTok.EACLTable()
	}

	hdrSrcOpts := make([]eaclV2.Option, 0, 3)

	hdrSrcOpts = append(hdrSrcOpts,
		eaclV2.WithLocalObjectStorage(c.localStorage),
		eaclV2.WithCID(cnr),
		eaclV2.WithOID(reqInfo.ObjectID()),
		eaclV2.WithHeaderSource(c.headerSource),
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

	vu := new(eaclSDK.ValidationUnit).
		WithRole(eaclRole).
		WithOperation(eaclSDK.Operation(reqInfo.Operation())).
		WithContainerID(&cnr).
		WithSenderKey(reqInfo.SenderKey()).
		WithHeaderSource(hdrSrc).
		WithEACLTable(&table)

	if sa := reqInfo.SenderAccount(); sa != nil && !sa.IsZero() {
		vu.WithAccount(*sa)
	}

	action, matched, err := c.validator.CalculateAction(vu)

	if err != nil {
		return err
	}

	if !matched {
		return v2.ErrNotMatched
	}

	if action != eaclSDK.ActionAllow {
		return errEACLDeniedByRule
	}
	return nil
}

func isOwnerFromKey(id user.ID, key []byte) bool {
	if key == nil {
		return false
	}

	pubKey, err := keys.NewPublicKeyFromBytes(key, elliptic.P256())
	if err != nil {
		return false
	}

	return id == user.NewFromECDSAPublicKey(ecdsa.PublicKey(*pubKey))
}
