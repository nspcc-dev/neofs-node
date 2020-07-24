package id

import (
	"errors"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
)

type OwnerID = container.OwnerID

// OwnerKeyContainer is an interface of the container of owner's ID and key pair with read access.
type OwnerKeyContainer interface {
	GetOwnerID() OwnerID
	GetOwnerKey() []byte
}

// ErrNilOwnerKeyContainer is returned by functions that expect a non-nil
// OwnerKeyContainer, but received nil.
var ErrNilOwnerKeyContainer = errors.New("owner-key container is nil")

// VerifyKey checks if the public key converts to owner ID.
//
// If passed OwnerKeyContainer is nil, ErrNilOwnerKeyContainer returns.
// If public key cannot be unmarshaled, service.ErrInvalidPublicKeyBytes returns.
// If public key is not converted to owner ID, service.ErrWrongOwner returns.
// With neo:morph adoption public key can be unrelated to owner ID. In this
// case VerifyKey should call NeoFS.ID smart-contract to check whether public
// key is bounded with owner ID. If there is no bound, then return
// service.ErrWrongOwner.
func VerifyKey(src OwnerKeyContainer) error {
	if src == nil {
		return ErrNilOwnerKeyContainer
	}

	pubKey := crypto.UnmarshalPublicKey(src.GetOwnerKey())
	if pubKey == nil {
		return service.ErrInvalidPublicKeyBytes
	}

	ownerFromKey, err := refs.NewOwnerID(pubKey)
	if err != nil {
		return err
	}

	if !ownerFromKey.Equal(src.GetOwnerID()) {
		return service.ErrWrongOwner
	}

	return nil
}
