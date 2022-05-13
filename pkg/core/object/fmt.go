package object

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
)

// FormatValidator represents an object format validator.
type FormatValidator struct {
	*cfg
}

// FormatValidatorOption represents a FormatValidator constructor option.
type FormatValidatorOption func(*cfg)

type cfg struct {
	deleteHandler DeleteHandler

	netState netmap.State

	locker Locker
}

// DeleteHandler is an interface of delete queue processor.
type DeleteHandler interface {
	// DeleteObjects places objects to a removal queue.
	//
	// Returns apistatus.LockNonRegularObject if at least one object
	// is locked.
	DeleteObjects(*addressSDK.Address, ...*addressSDK.Address) error
}

// Locker is an object lock storage interface.
type Locker interface {
	// Lock list of objects as locked by locker in the specified container.
	//
	// Returns apistatus.LockNonRegularObject if at least object in locked
	// list is irregular (not type of REGULAR).
	Lock(idCnr cid.ID, locker oid.ID, locked []oid.ID) error
}

var errNilObject = errors.New("object is nil")

var errNilID = errors.New("missing identifier")

var errNilCID = errors.New("missing container identifier")

var errNoExpirationEpoch = errors.New("missing expiration epoch attribute")

var errTombstoneExpiration = errors.New("tombstone body and header contain different expiration values")

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

	_, idSet := obj.ID()
	if !unprepared && !idSet {
		return errNilID
	}

	_, cnrSet := obj.ContainerID()
	if !cnrSet {
		return errNilCID
	}

	if err := v.checkOwner(obj); err != nil {
		return err
	}

	if err := v.checkAttributes(obj); err != nil {
		return fmt.Errorf("invalid attributes: %w", err)
	}

	if !unprepared {
		if err := v.validateSignatureKey(obj); err != nil {
			return fmt.Errorf("(%T) could not validate signature key: %w", v, err)
		}

		if err := v.checkExpiration(obj); err != nil {
			return fmt.Errorf("object did not pass expiration check: %w", err)
		}

		if err := object.CheckHeaderVerificationFields(obj); err != nil {
			return fmt.Errorf("(%T) could not validate header fields: %w", v, err)
		}
	}

	if obj = obj.Parent(); obj != nil {
		// Parent object already exists.
		return v.Validate(obj, false)
	}

	return nil
}

func (v *FormatValidator) validateSignatureKey(obj *object.Object) error {
	token := obj.SessionToken()
	key := obj.Signature().Key()

	if token == nil || !bytes.Equal(token.SessionKey(), key) {
		return v.checkOwnerKey(obj.OwnerID(), obj.Signature().Key())
	}

	// FIXME: #1159 perform token verification

	return nil
}

func (v *FormatValidator) checkOwnerKey(id *owner.ID, key []byte) error {
	pub, err := keys.NewPublicKeyFromBytes(key, elliptic.P256())
	if err != nil {
		return err
	}

	id2 := owner.NewIDFromPublicKey((*ecdsa.PublicKey)(pub))

	if !id.Equal(id2) {
		return fmt.Errorf("(%T) different owner identifiers %s/%s", v, id, id2)
	}

	return nil
}

// ValidateContent validates payload content according to the object type.
func (v *FormatValidator) ValidateContent(o *object.Object) error {
	switch o.Type() {
	case object.TypeRegular:
		// ignore regular objects, they do not need payload formatting
	case object.TypeTombstone:
		if len(o.Payload()) == 0 {
			return fmt.Errorf("(%T) empty payload in tombstone", v)
		}

		tombstone := object.NewTombstone()

		if err := tombstone.Unmarshal(o.Payload()); err != nil {
			return fmt.Errorf("(%T) could not unmarshal tombstone content: %w", v, err)
		}

		// check if the tombstone has the same expiration in the body and the header
		exp, err := expirationEpochAttribute(o)
		if err != nil {
			return err
		}

		if exp != tombstone.ExpirationEpoch() {
			return errTombstoneExpiration
		}

		// mark all objects from the tombstone body as removed in the storage engine
		cnr, ok := o.ContainerID()
		if !ok {
			return errors.New("missing container ID")
		}

		idList := tombstone.Members()
		addrList := make([]*addressSDK.Address, 0, len(idList))

		for i := range idList {
			a := addressSDK.NewAddress()
			a.SetContainerID(cnr)
			a.SetObjectID(idList[i])

			addrList = append(addrList, a)
		}

		if v.deleteHandler != nil {
			err = v.deleteHandler.DeleteObjects(AddressOf(o), addrList...)
			if err != nil {
				return fmt.Errorf("delete objects from %s object content: %w", o.Type(), err)
			}
		}
	case object.TypeStorageGroup:
		if len(o.Payload()) == 0 {
			return fmt.Errorf("(%T) empty payload in SG", v)
		}

		var sg storagegroup.StorageGroup

		if err := sg.Unmarshal(o.Payload()); err != nil {
			return fmt.Errorf("(%T) could not unmarshal SG content: %w", v, err)
		}
	case object.TypeLock:
		if len(o.Payload()) == 0 {
			return errors.New("empty payload in lock")
		}

		cnr, ok := o.ContainerID()
		if !ok {
			return errors.New("missing container")
		}

		id, ok := o.ID()
		if !ok {
			return errors.New("missing ID")
		}

		var lock object.Lock

		err := lock.Unmarshal(o.Payload())
		if err != nil {
			return fmt.Errorf("decode lock payload: %w", err)
		}

		if v.locker != nil {
			num := lock.NumberOfMembers()
			if num == 0 {
				return errors.New("missing locked members")
			}

			// mark all objects from lock list as locked in the storage engine
			locklist := make([]oid.ID, num)
			lock.ReadMembers(locklist)

			err = v.locker.Lock(cnr, id, locklist)
			if err != nil {
				return fmt.Errorf("lock objects from %s object content: %w", o.Type(), err)
			}
		}
	default:
		// ignore all other object types, they do not need payload formatting
	}

	return nil
}

var errExpired = errors.New("object has expired")

func (v *FormatValidator) checkExpiration(obj *object.Object) error {
	exp, err := expirationEpochAttribute(obj)
	if err != nil {
		if errors.Is(err, errNoExpirationEpoch) {
			return nil // objects without expiration attribute are valid
		}

		return err
	}

	if exp < v.netState.CurrentEpoch() {
		return errExpired
	}

	return nil
}

func expirationEpochAttribute(obj *object.Object) (uint64, error) {
	for _, a := range obj.Attributes() {
		if a.Key() != objectV2.SysAttributeExpEpoch {
			continue
		}

		return strconv.ParseUint(a.Value(), 10, 64)
	}

	return 0, errNoExpirationEpoch
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
	// TODO: use an appropriate functionality after neofs-api-go#352
	if len(obj.OwnerID().ToV2().GetValue()) != owner.NEO3WalletSize {
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

// WithDeleteHandler returns an option to set delete queue processor.
func WithDeleteHandler(v DeleteHandler) FormatValidatorOption {
	return func(c *cfg) {
		c.deleteHandler = v
	}
}

// WithLocker returns an option to set object lock storage.
func WithLocker(v Locker) FormatValidatorOption {
	return func(c *cfg) {
		c.locker = v
	}
}
