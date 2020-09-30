package object

import (
	"bytes"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/pkg/errors"
)

// FormatValidator represents object format validator.
type FormatValidator struct{}

var errNilObject = errors.New("object is nil")

var errNilID = errors.New("missing identifier")

var errNilCID = errors.New("missing container identifier")

// NewFormatValidator creates, initializes and returns FormatValidator instance.
func NewFormatValidator() *FormatValidator {
	return new(FormatValidator)
}

// Validate validates object format.
//
// Returns nil error if object has valid structure.
func (v *FormatValidator) Validate(obj *Object) error {
	if obj == nil {
		return errNilObject
	} else if obj.GetID() == nil {
		return errNilID
	} else if obj.GetContainerID() == nil {
		return errNilCID
	}

	if err := v.validateContent(obj.GetType(), obj.GetPayload()); err != nil {
		return errors.Wrapf(err, "(%T) incorrect content", v)
	}

	if err := v.validateSignatureKey(obj); err != nil {
		return errors.Wrapf(err, "(%T) could not validate signature key", v)
	}

	if err := object.CheckHeaderVerificationFields(obj.SDK()); err != nil {
		return errors.Wrapf(err, "(%T) could not validate header fields", v)
	}

	par := NewFromSDK(obj.GetParent())

	// validate parent object header
	if par.GetID() != nil && len(obj.GetChildren()) == 0 {
		return v.Validate(par)
	}

	return nil
}

func (v *FormatValidator) validateSignatureKey(obj *Object) error {
	token := obj.GetSessionToken()
	key := obj.GetSignature().GetKey()

	if token == nil || !bytes.Equal(token.SessionKey(), key) {
		return v.checkOwnerKey(obj.GetOwnerID(), obj.GetSignature().GetKey())
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

func (v *FormatValidator) validateContent(t object.Type, payload []byte) error {
	switch t {
	case object.TypeTombstone:
		if len(payload) == 0 {
			return errors.Errorf("(%T) empty payload in tombstone", v)
		}

		addr, err := object.AddressFromBytes(payload)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not parse object address from tombstone", v)
		}

		if addr.GetContainerID() == nil || addr.GetObjectID() == nil {
			return errors.Errorf("(%T) empty address reference in tombstone", v)
		}
	}

	return nil
}
