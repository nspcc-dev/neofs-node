package crypto

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// AuthenticateObject checks whether obj is signed correctly by its owner.
func AuthenticateObject(obj object.Object) error {
	sig := obj.Signature()
	if sig == nil {
		return errMissingSignature
	}
	_, err := verifySignature(*sig, obj.SignedData)
	return err
}
