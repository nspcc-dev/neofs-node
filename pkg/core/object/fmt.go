package object

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
)

// FormatValidator represents an object format validator.
type FormatValidator struct {
	*cfg
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
}

var errNilObject = errors.New("object is nil")

var errNilID = errors.New("missing identifier")

var errNilCID = errors.New("missing container identifier")

var errTombstoneExpiration = errors.New("tombstone body and header contain different expiration values")

var errEmptySGMembers = errors.New("storage group with empty members list")

func defaultCfg() *cfg {
	return new(cfg)
}

// NewFormatValidator creates, initializes and returns FormatValidator instance.
func NewFormatValidator(opts ...FormatValidatorOption) *FormatValidator {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	return &FormatValidator{
		cfg: cfg,
	}
}

// Validate validates object format.
//
// Does not validate payload checksum and content.
// If unprepared is true, only fields set by user are validated.
//
// Returns nil error if the object has valid structure.
func (v *FormatValidator) Validate(obj *object.Object, unprepared bool) error {
	if obj == nil {
		return errNilObject
	}

	if !unprepared && obj.GetID().IsZero() {
		return errNilID
	}

	if obj.GetContainerID().IsZero() {
		return errNilCID
	}

	if err := v.checkOwner(obj); err != nil {
		return err
	}

	_, firstSet := obj.FirstID()
	splitID := obj.SplitID()
	par := obj.Parent()

	if obj.HasParent() {
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
		if err := v.validateSignatureKey(obj); err != nil {
			return fmt.Errorf("(%T) could not validate signature key: %w", v, err)
		}

		if err := v.checkExpiration(*obj); err != nil {
			return fmt.Errorf("object did not pass expiration check: %w", err)
		}

		if err := obj.CheckHeaderVerificationFields(); err != nil {
			return fmt.Errorf("(%T) could not validate header fields: %w", v, err)
		}
	}

	if par != nil && (firstSet || splitID != nil) {
		// Parent object already exists.
		return v.Validate(par, false)
	}

	return nil
}

func (v *FormatValidator) validateSignatureKey(obj *object.Object) error {
	// FIXME(@cthulhu-rider): temp solution, see neofs-sdk-go#233
	sig := obj.Signature()
	if sig == nil {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return errors.New("missing signature")
	}

	token := obj.SessionToken()
	if token == nil {
		return nil
	}

	if sig.PublicKey() == nil {
		return errors.New("missing public key")
	}
	if !token.AssertAuthKey(sig.PublicKey()) {
		return errors.New("session token is not for object's signer")
	}

	if !token.VerifySignature() {
		return errors.New("incorrect session token signature")
	}

	if issuer, owner := token.Issuer(), obj.OwnerID(); issuer != *owner { // nil check was performed above
		return fmt.Errorf("different object owner %s and session issuer %s", owner, issuer)
	}

	return nil
}

// ContentMeta describes NeoFS meta information that brings object's payload if the object
// is one of:
//   - object.TypeTombstone;
//   - object.TypeStorageGroup;
//   - object.TypeLink;
//   - object.TypeLock.
type ContentMeta struct {
	typ object.Type

	objs []oid.ID
}

// Type returns object's type.
func (i ContentMeta) Type() object.Type {
	return i.typ
}

// Objects returns objects that the original object's payload affects:
//   - inhumed objects, if the original object is a Tombstone;
//   - locked objects, if the original object is a Lock;
//   - members of a storage group, if the original object is a Storage group;
//   - nil, if the original object is a Regular object.
func (i ContentMeta) Objects() []oid.ID {
	return i.objs
}

// ValidateContent validates payload content according to the object type.
func (v *FormatValidator) ValidateContent(o *object.Object) (ContentMeta, error) {
	meta := ContentMeta{
		typ: o.Type(),
	}

	switch o.Type() {
	case object.TypeRegular:
		// ignore regular objects, they do not need payload formatting
	case object.TypeLink:
		if len(o.Payload()) == 0 {
			return ContentMeta{}, fmt.Errorf("(%T) empty payload in the link object", v)
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
		if len(o.Payload()) == 0 {
			return ContentMeta{}, fmt.Errorf("(%T) empty payload in tombstone", v)
		}

		tombstone := object.NewTombstone()

		if err := tombstone.Unmarshal(o.Payload()); err != nil {
			return ContentMeta{}, fmt.Errorf("(%T) could not unmarshal tombstone content: %w", v, err)
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
	case object.TypeStorageGroup:
		if len(o.Payload()) == 0 {
			return ContentMeta{}, fmt.Errorf("(%T) empty payload in SG", v)
		}

		var sg storagegroup.StorageGroup

		if err := sg.Unmarshal(o.Payload()); err != nil {
			return ContentMeta{}, fmt.Errorf("(%T) could not unmarshal SG content: %w", v, err)
		}

		mm := sg.Members()
		meta.objs = mm

		lenMM := len(mm)
		if lenMM == 0 {
			return ContentMeta{}, errEmptySGMembers
		}

		uniqueFilter := make(map[oid.ID]struct{}, lenMM)

		for i := range lenMM {
			if _, alreadySeen := uniqueFilter[mm[i]]; alreadySeen {
				return ContentMeta{}, fmt.Errorf("storage group contains non-unique member: %s", mm[i])
			}

			uniqueFilter[mm[i]] = struct{}{}
		}
	case object.TypeLock:
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

		// check that LOCK object has correct expiration epoch
		lockExp, err := Expiration(*o)
		if err != nil {
			return ContentMeta{}, fmt.Errorf("lock object expiration epoch: %w", err)
		}

		if currEpoch := v.netState.CurrentEpoch(); lockExp < currEpoch {
			return ContentMeta{}, fmt.Errorf("lock object expiration: %d; current: %d", lockExp, currEpoch)
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
)

func (v *FormatValidator) checkAttributes(obj *object.Object) error {
	as := obj.Attributes()

	mUnique := make(map[string]struct{}, len(as))

	for _, a := range as {
		key := a.Key()

		if _, was := mUnique[key]; was {
			return errDuplAttr
		}

		if a.Value() == "" {
			return errEmptyAttrVal
		}

		mUnique[key] = struct{}{}
	}

	return nil
}

var errIncorrectOwner = errors.New("incorrect object owner")

func (v *FormatValidator) checkOwner(obj *object.Object) error {
	if idOwner := obj.OwnerID(); idOwner == nil {
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
