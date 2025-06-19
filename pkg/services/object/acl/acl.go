package acl

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	eaclV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl/v2"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// FSChain provides base non-contract functionality of the FS chain required for
// [Checker] to work.
type FSChain interface {
	InvokeContainedScript(tx *transaction.Transaction, header *block.Header, _ *trigger.Type, _ *bool) (*result.Invoke, error)
}

// NetmapContract represents Netmap contract deployed in the FS chain required
// for [Checker] to work.
type NetmapContract interface {
	// GetEpochBlock returns FS chain height when given NeoFS epoch was ticked.
	GetEpochBlock(epoch uint64) (uint32, error)
}

// CheckerPrm groups parameters for Checker
// constructor.
type CheckerPrm struct {
	eaclSrc        container.EACLSource
	validator      *eaclSDK.Validator
	localStorage   *engine.StorageEngine
	state          netmap.State
	headerSource   eaclV2.HeaderSource
	fsChain        FSChain
	netmapContract NetmapContract
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

func (c *CheckerPrm) SetHeaderSource(hs eaclV2.HeaderSource) *CheckerPrm {
	c.headerSource = hs
	return c
}

func (c *CheckerPrm) SetFSChain(fsChain FSChain) *CheckerPrm {
	c.fsChain = fsChain
	return c
}

func (c *CheckerPrm) SetNetmapContract(nc NetmapContract) *CheckerPrm {
	c.netmapContract = nc
	return c
}

// Checker implements v2.ACLChecker interfaces and provides
// ACL/eACL validation functionality.
type Checker struct {
	eaclSrc        container.EACLSource
	validator      *eaclSDK.Validator
	localStorage   *engine.StorageEngine
	state          netmap.State
	headerSource   eaclV2.HeaderSource
	fsChain        FSChain
	netmapContract NetmapContract

	bearerTokenCommonCheckCache *lru.Cache[[sha256.Size]byte, error]
}

type historicN3ScriptRunner struct {
	FSChain
	NetmapContract
}

// Various EACL check errors.
var (
	errEACLDeniedByRule         = errors.New("denied by rule")
	errBearerExpired            = errors.New("bearer token has expired")
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
	panicOnNil("HeaderSource", prm.headerSource)

	bearerTokenCheckCache, err := lru.New[[sha256.Size]byte, error](1000)
	if err != nil {
		panic(fmt.Errorf("unexpected error in lru.New: %w", err))
	}

	return &Checker{
		eaclSrc:                     prm.eaclSrc,
		validator:                   prm.validator,
		localStorage:                prm.localStorage,
		state:                       prm.state,
		headerSource:                prm.headerSource,
		fsChain:                     prm.fsChain,
		netmapContract:              prm.netmapContract,
		bearerTokenCommonCheckCache: bearerTokenCheckCache,
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

	if bt := reqInfo.Bearer(); bt != nil {
		if err := c.isValidBearer(*bt, reqInfo.ContainerID(), reqInfo.ContainerOwner(), *reqInfo.SenderAccount()); err != nil {
			return err
		}
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

	action, _, err := c.validator.CalculateAction(vu)

	if err != nil {
		return err
	}

	if action != eaclSDK.ActionAllow {
		return errEACLDeniedByRule
	}
	return nil
}

// ResetBearerTokenCheckCache resets cache of bearer token check results.
func (c *Checker) ResetBearerTokenCheckCache() {
	c.bearerTokenCommonCheckCache.Purge()
}

// isValidBearer checks whether bearer token was correctly signed by authorized
// entity. This method might be defined on whole ACL service because it will
// require fetching current epoch to check lifetime.
func (c *Checker) isValidBearer(token bearer.Token, reqCnr cid.ID, ownerCnr user.ID, usrSender user.ID) error {
	cacheKey := sha256.Sum256(token.Marshal())
	err, ok := c.bearerTokenCommonCheckCache.Get(cacheKey)
	if !ok {
		// TODO: Signed data is used twice - for cache key and to check the signature. Coding can be deduplicated.
		err = c.verifyBearerTokenCommon(token, ownerCnr)
		c.bearerTokenCommonCheckCache.Add(cacheKey, err)
	}
	if err != nil {
		return err
	}

	return c.verifyBearerTokenAgainstRequest(token, reqCnr, usrSender)
}

func (c *Checker) verifyBearerTokenCommon(token bearer.Token, ownerCnr user.ID) error {
	if !token.ValidAt(c.state.CurrentEpoch()) {
		return errBearerExpired
	}

	if err := icrypto.AuthenticateToken(&token, historicN3ScriptRunner{
		FSChain:        c.fsChain,
		NetmapContract: c.netmapContract,
	}); err != nil {
		return fmt.Errorf("authenticate bearer token: %w", err)
	}

	if token.ResolveIssuer() != ownerCnr {
		return errBearerNotSignedByOwner
	}

	return nil
}

func (c *Checker) verifyBearerTokenAgainstRequest(token bearer.Token, reqCnr cid.ID, reqSender user.ID) error {
	cnr := token.EACLTable().GetCID()
	if !cnr.IsZero() && cnr != reqCnr {
		return errBearerInvalidContainerID
	}

	if !token.AssertUser(reqSender) {
		return errBearerInvalidOwner
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
