package object

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// FormatValidator represents an object format validator.
type FormatValidator struct {
	*cfg
	fsChain        FSChain
	netmapContract NetmapContract
	containers     container.Source
}

// FormatValidatorOption represents a FormatValidator constructor option.
type FormatValidatorOption func(*cfg)

type cfg struct {
	netState netmap.State
	e        LockSource
	sv       SplitVerifier
	tv       TombVerifier
}

// DeleteHandler is an interface of delete queue processor.
type DeleteHandler interface {
	// DeleteObjects places objects to a removal queue.
	//
	// Returns apistatus.LockNonRegularObject if at least one object
	// is locked.
	DeleteObjects(oid.Address, ...oid.Address) error
}

// LockSource is a source of lock relations between the objects.
type LockSource interface {
	// IsLocked must clarify object's lock status.
	IsLocked(address oid.Address) (bool, error)
}

// Locker is an object lock storage interface.
type Locker interface {
	// Lock list of objects as locked by locker in the specified container.
	//
	// Returns apistatus.LockNonRegularObject if at least object in locked
	// list is irregular (not type of REGULAR).
	Lock(idCnr cid.ID, locker oid.ID, locked []oid.ID) error
}

// SplitVerifier represent split validation unit. It verifies V2 split
// chains based on the link object's payload (list of ID+ObjectSize pairs).
type SplitVerifier interface {
	// VerifySplit must verify split hierarchy and return any error that did
	// not allow processing the chain. Must break (if possible) any internal
	// computations if context is done. The second and the third args are the
	// first part's address used as a chain unique identifier that also must
	// be checked. The fourth arg is guaranteed to be the full list from the
	// link's payload without item order change.
	VerifySplit(context.Context, cid.ID, oid.ID, []object.MeasuredObject) error
}

// TombVerifier represents tombstone validation unit. It verifies tombstone
// object received by the node.
type TombVerifier interface {
	// VerifyTomb must verify tombstone payload. Must break (if possible) any internal
	// computations if context is done.
	VerifyTomb(ctx context.Context, cnr cid.ID, t object.Tombstone) error

	// VerifyTombStoneWithoutPayload must verify API 2.18+ tombstones without
	// payload.
	VerifyTombStoneWithoutPayload(ctx context.Context, t object.Object) error
}

// FSChain provides base non-contract functionality of the FS chain required for
// [FormatValidator] to work.
type FSChain interface {
	InvokeContainedScript(tx *transaction.Transaction, header *block.Header, _ *trigger.Type, _ *bool) (*result.Invoke, error)
}

// NetmapContract represents Netmap contract deployed in the FS chain required
// for [FormatValidator] to work.
type NetmapContract interface {
	// GetEpochBlock returns FS chain height when given NeoFS epoch was ticked.
	GetEpochBlock(epoch uint64) (uint32, error)
}

type historicN3ScriptRunner struct {
	FSChain
	NetmapContract
}

var errNilObject = errors.New("object is nil")

var errNilID = errors.New("missing identifier")

var errNilCID = errors.New("missing container identifier")

var errTombstoneExpiration = errors.New("tombstone body and header contain different expiration values")

func defaultCfg() *cfg {
	return new(cfg)
}

// NewFormatValidator creates, initializes and returns FormatValidator instance.
func NewFormatValidator(fsChain FSChain, netmapContract NetmapContract, containers container.Source, opts ...FormatValidatorOption) *FormatValidator {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	return &FormatValidator{
		cfg:            cfg,
		fsChain:        fsChain,
		netmapContract: netmapContract,
		containers:     containers,
	}
}

// Validate validates object format.
//
// Does not validate payload checksum and content.
// If unprepared is true, only fields set by user are validated.
//
// Returns nil error if the object has valid structure.
func (v *FormatValidator) Validate(obj *object.Object, unprepared bool) error {
	return v.validate(obj, unprepared, false)
}

func (v *FormatValidator) validate(obj *object.Object, unprepared, isParent bool) error {
	if obj == nil {
		return errNilObject
	}

	switch obj.Type() {
	case object.TypeStorageGroup: //nolint:staticcheck // TypeStorageGroup is deprecated and that's exactly what we want to check here.
		return fmt.Errorf("strorage group type is no longer supported")
	case object.TypeLock, object.TypeTombstone:
		if !unprepared && version.SysObjTargetShouldBeInHeader(obj.Version()) {
			if len(obj.Payload()) > 0 {
				return errors.New("system object has payload")
			}
			if obj.AssociatedObject().IsZero() {
				return errors.New("system object has zero associated object")
			}
		}
	default:
	}

	var hdrLen = obj.HeaderLen()
	if hdrLen > object.MaxHeaderLen {
		return fmt.Errorf("object header length exceeds the limit: %d>%d", hdrLen, object.MaxHeaderLen)
	}

	if !unprepared && obj.GetID().IsZero() {
		return errNilID
	}

	cnrID := obj.GetContainerID()
	if cnrID.IsZero() {
		return errNilCID
	}

	if err := v.checkOwner(obj); err != nil {
		return err
	}

	cnr, err := v.containers.Get(cnrID)
	if err != nil {
		return fmt.Errorf("read container by ID=%s: %w", cnrID, err)
	}

	isEC, err := checkEC(*obj, cnr.PlacementPolicy().ECRules(), unprepared, isParent)
	if err != nil {
		return err
	}

	_, firstSet := obj.FirstID()
	splitID := obj.SplitID()
	par := obj.Parent()

	if !isEC && obj.HasParent() {
		if par != nil && par.HasParent() {
			return errors.New("parent object has a parent itself")
		}

		if splitID != nil {
			// V1 split
			if firstSet {
				return errors.New("v1 split: first object ID is set")
			}
		} else {
			// V2 split

			if firstSet {
				// 2nd+ parts

				typ := obj.Type()

				// link object only
				if typ == object.TypeLink && (par == nil || par.Signature() == nil) {
					return errors.New("v2 split: incorrect link object's parent header")
				}

				if prevID := obj.GetPreviousID(); typ != object.TypeLink && prevID.IsZero() {
					return errors.New("v2 split: middle part does not have previous object ID")
				}
			}
		}
	}

	if err := v.checkAttributes(obj); err != nil {
		return fmt.Errorf("invalid attributes: %w", err)
	}

	if !unprepared {
		if err := obj.VerifyID(); err != nil {
			return fmt.Errorf("could not validate header fields: invalid identifier: %w", err)
		}

		if err := icrypto.AuthenticateObject(*obj, historicN3ScriptRunner{
			FSChain:        v.fsChain,
			NetmapContract: v.netmapContract,
		}); err != nil {
			return fmt.Errorf("authenticate: %w", err)
		}

		if err := v.checkExpiration(*obj); err != nil {
			return fmt.Errorf("object did not pass expiration check: %w", err)
		}
	}

	if par != nil && (firstSet || splitID != nil || isEC) {
		// Parent object already exists.
		return v.validate(par, false, true)
	}

	return nil
}

// ContentMeta describes NeoFS meta information that brings object's payload if the object
// is one of:
//   - object.TypeTombstone;
//   - object.TypeLink;
//   - object.TypeLock.
type ContentMeta struct {
	objs []oid.ID
}

// Objects returns objects that the original object's payload affects:
//   - inhumed objects, if the original object is a Tombstone;
//   - locked objects, if the original object is a Lock;
//   - nil, if the original object is a Regular object.
func (i ContentMeta) Objects() []oid.ID {
	return i.objs
}

// ValidateContent validates payload content according to the object type.
func (v *FormatValidator) ValidateContent(o *object.Object) (ContentMeta, error) {
	var meta ContentMeta

	switch o.Type() {
	case object.TypeRegular:
		// ignore regular objects, they do not need payload formatting
	case object.TypeLink:
		if len(o.Payload()) == 0 {
			return ContentMeta{}, errors.New("empty payload in the link object")
		}

		firstObjID, set := o.FirstID()
		if !set {
			return ContentMeta{}, errors.New("link object does not have first object ID")
		}

		cnr := o.GetContainerID()
		if cnr.IsZero() {
			return ContentMeta{}, errors.New("link object does not have container ID")
		}

		var testLink object.Link

		err := o.ReadLink(&testLink)
		if err != nil {
			return ContentMeta{}, fmt.Errorf("reading link object's payload: %w", err)
		}

		err = v.sv.VerifySplit(context.Background(), cnr, firstObjID, testLink.Objects())
		if err != nil {
			return ContentMeta{}, fmt.Errorf("link object's split chain verification: %w", err)
		}
	case object.TypeTombstone:
		if version.SysObjTargetShouldBeInHeader(o.Version()) {
			return ContentMeta{}, v.tv.VerifyTombStoneWithoutPayload(context.Background(), *o)
		}

		if len(o.Payload()) == 0 {
			return ContentMeta{}, errors.New("empty payload in tombstone")
		}

		tombstone := object.NewTombstone()

		if err := tombstone.Unmarshal(o.Payload()); err != nil {
			return ContentMeta{}, fmt.Errorf("could not unmarshal tombstone content: %w", err)
		}

		// check if the tombstone has the same expiration in the body and the header
		exp, err := Expiration(*o)
		if err != nil {
			return ContentMeta{}, err
		}

		if exp != tombstone.ExpirationEpoch() {
			return ContentMeta{}, errTombstoneExpiration
		}

		cnr := o.GetContainerID()
		if cnr.IsZero() {
			return ContentMeta{}, errors.New("missing container ID")
		}

		err = v.tv.VerifyTomb(context.Background(), cnr, *tombstone)
		if err != nil {
			return ContentMeta{}, fmt.Errorf("tombstone verification: %w", err)
		}

		idList := tombstone.Members()
		meta.objs = idList
	case object.TypeLock:
		// check that LOCK object has correct expiration epoch
		lockExp, err := Expiration(*o)
		if err != nil {
			return ContentMeta{}, fmt.Errorf("lock object expiration epoch: %w", err)
		}

		if currEpoch := v.netState.CurrentEpoch(); lockExp < currEpoch {
			return ContentMeta{}, fmt.Errorf("lock object expiration: %d; current: %d", lockExp, currEpoch)
		}

		if version.SysObjTargetShouldBeInHeader(o.Version()) {
			return ContentMeta{}, nil
		}

		if len(o.Payload()) == 0 {
			return ContentMeta{}, errors.New("empty payload in lock")
		}

		cnr := o.GetContainerID()
		if cnr.IsZero() {
			return ContentMeta{}, errors.New("missing container")
		}

		objID := o.GetID()
		if objID.IsZero() {
			return ContentMeta{}, errors.New("missing ID")
		}

		var lock object.Lock

		err = lock.Unmarshal(o.Payload())
		if err != nil {
			return ContentMeta{}, fmt.Errorf("decode lock payload: %w", err)
		}

		num := lock.NumberOfMembers()
		if num == 0 {
			return ContentMeta{}, errors.New("missing locked members")
		}

		meta.objs = make([]oid.ID, num)
		lock.ReadMembers(meta.objs)
	default:
		// ignore all other object types, they do not need payload formatting
	}

	return meta, nil
}

var errExpired = errors.New("object has expired")

func (v *FormatValidator) checkExpiration(obj object.Object) error {
	exp, err := Expiration(obj)
	if err != nil {
		if errors.Is(err, ErrNoExpiration) {
			return nil // objects without expiration attribute are valid
		}

		return err
	}

	if currEpoch := v.netState.CurrentEpoch(); exp < currEpoch {
		// an object could be expired but locked;
		// put such an object is a correct operation

		cID := obj.GetContainerID()
		oID := obj.GetID()

		var addr oid.Address
		addr.SetContainer(cID)
		addr.SetObject(oID)

		locked, err := v.e.IsLocked(addr)
		if err != nil {
			return fmt.Errorf("locking status check for an expired object: %w", err)
		}

		if !locked {
			return fmt.Errorf("%w: attribute: %d, current: %d", errExpired, exp, currEpoch)
		}
	}

	return nil
}

var (
	errDuplAttr     = errors.New("duplication of attributes detected")
	errEmptyAttrVal = errors.New("empty attribute value")
	errZeroByte     = errors.New("illegal zero byte")
)

func checkZeroByte(k, v string) error {
	if strings.IndexByte(k, 0x00) >= 0 {
		return fmt.Errorf("invalid key: %w", errZeroByte)
	}
	if strings.IndexByte(v, 0x00) >= 0 {
		return fmt.Errorf("invalid value: %w", errZeroByte)
	}
	return nil
}

func (v *FormatValidator) checkAttributes(obj *object.Object) error {
	as := obj.Attributes()

	mUnique := make(map[string]struct{}, len(as))

	for i, a := range as {
		key := a.Key()

		if _, was := mUnique[key]; was {
			return errDuplAttr
		}

		val := a.Value()
		if a.Value() == "" {
			return errEmptyAttrVal
		}

		if err := checkZeroByte(key, val); err != nil {
			return fmt.Errorf("invalid attribute #%d: %w", i, err)
		}

		mUnique[key] = struct{}{}
	}

	return nil
}

var errIncorrectOwner = errors.New("incorrect object owner")

func (v *FormatValidator) checkOwner(obj *object.Object) error {
	if obj.Owner().IsZero() {
		return errIncorrectOwner
	}

	return nil
}

// WithNetState returns options to set the network state interface.
func WithNetState(netState netmap.State) FormatValidatorOption {
	return func(c *cfg) {
		c.netState = netState
	}
}

// WithLockSource return option to set a Locked objects source.
func WithLockSource(e LockSource) FormatValidatorOption {
	return func(c *cfg) {
		c.e = e
	}
}

// WithSplitVerifier returns option to set a SplitVerifier.
func WithSplitVerifier(sv SplitVerifier) FormatValidatorOption {
	return func(c *cfg) {
		c.sv = sv
	}
}

// WithTombVerifier returns option to set a TombVerifier.
func WithTombVerifier(tv TombVerifier) FormatValidatorOption {
	return func(c *cfg) {
		c.tv = tv
	}
}
