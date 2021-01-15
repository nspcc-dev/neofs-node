package object

import (
	"bytes"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/pkg/errors"
)

// FormatValidator represents object format validator.
type FormatValidator struct {
	*cfg
}

// FormatValidatorOption represents FormatValidator constructor option.
type FormatValidatorOption func(*cfg)

type cfg struct {
	deleteHandler DeleteHandler
}

// DeleteHandler is an interface of delete queue processor.
type DeleteHandler interface {
	DeleteObjects(*object.Address, ...*object.Address)
}

var errNilObject = errors.New("object is nil")

var errNilID = errors.New("missing identifier")

var errNilCID = errors.New("missing container identifier")

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
//
// Returns nil error if object has valid structure.
func (v *FormatValidator) Validate(obj *Object) error {
	if obj == nil {
		return errNilObject
	} else if obj.ID() == nil {
		return errNilID
	} else if obj.ContainerID() == nil {
		return errNilCID
	}

	for ; obj != nil; obj = obj.GetParent() {
		if err := v.validateSignatureKey(obj); err != nil {
			return errors.Wrapf(err, "(%T) could not validate signature key", v)
		}

		if err := object.CheckHeaderVerificationFields(obj.SDK()); err != nil {
			return errors.Wrapf(err, "(%T) could not validate header fields", v)
		}
	}

	return nil
}

func (v *FormatValidator) validateSignatureKey(obj *Object) error {
	token := obj.SessionToken()
	key := obj.Signature().Key()

	if token == nil || !bytes.Equal(token.SessionKey(), key) {
		return v.checkOwnerKey(obj.OwnerID(), obj.Signature().Key())
	}

	// FIXME: perform token verification

	return nil
}

func (v *FormatValidator) checkOwnerKey(id *owner.ID, key []byte) error {
	wallet, err := owner.NEO3WalletFromPublicKey(crypto.UnmarshalPublicKey(key))
	if err != nil {
		// TODO: check via NeoFSID
		return err
	}

	id2 := owner.NewID()
	id2.SetNeo3Wallet(wallet)

	// FIXME: implement Equal method
	if s1, s2 := id.String(), id2.String(); s1 != s2 {
		return errors.Errorf("(%T) different owner identifiers %s/%s", v, s1, s2)
	}

	return nil
}

// ValidateContent validates payload content according to object type.
func (v *FormatValidator) ValidateContent(o *Object) error {
	switch o.Type() {
	case object.TypeTombstone:
		if len(o.Payload()) == 0 {
			return errors.Errorf("(%T) empty payload in tombstone", v)
		}

		tombstone := object.NewTombstone()

		if err := tombstone.Unmarshal(o.Payload()); err != nil {
			return errors.Wrapf(err, "(%T) could not unmarshal tombstone content", v)
		}

		cid := o.ContainerID()
		idList := tombstone.Members()
		addrList := make([]*object.Address, 0, len(idList))

		for _, id := range idList {
			if id == nil {
				return errors.Errorf("(%T) empty member in tombstone", v)
			}

			a := object.NewAddress()
			a.SetContainerID(cid)
			a.SetObjectID(id)

			addrList = append(addrList, a)
		}

		if v.deleteHandler != nil {
			v.deleteHandler.DeleteObjects(o.Address(), addrList...)
		}
	case object.TypeStorageGroup:
		if len(o.Payload()) == 0 {
			return errors.Errorf("(%T) empty payload in SG", v)
		}

		sg := storagegroup.New()

		if err := sg.Unmarshal(o.Payload()); err != nil {
			return errors.Wrapf(err, "(%T) could not unmarshal SG content", v)
		}

		for _, id := range sg.Members() {
			if id == nil {
				return errors.Errorf("(%T) empty member in SG", v)
			}
		}
	default:
		// ignore all other object types, they do not need payload formatting
	}

	return nil
}

// WithDeleteHandler returns option to set delete queue processor.
func WithDeleteHandler(v DeleteHandler) FormatValidatorOption {
	return func(c *cfg) {
		c.deleteHandler = v
	}
}
