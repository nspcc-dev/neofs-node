package core

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/internal"
)

// OwnerKeyContainer is an interface of the container of owner's ID and key pair with read access.
type OwnerKeyContainer interface {
	GetOwnerID() refs.OwnerID
	GetOwnerKey() []byte
}

// OwnerKeyVerifier is an interface of OwnerKeyContainer validator.
type OwnerKeyVerifier interface {
	// Must check if OwnerKeyContainer satisfies a certain criterion.
	// Nil error is equivalent to matching the criterion.
	VerifyKey(context.Context, OwnerKeyContainer) error
}

type neoKeyVerifier struct{}

// ErrNilOwnerKeyContainer is returned by functions that expect a non-nil
// OwnerKeyContainer, but received nil.
const ErrNilOwnerKeyContainer = internal.Error("owner-key container is nil")

// ErrNilOwnerKeyVerifier is returned by functions that expect a non-nil
// OwnerKeyVerifier, but received nil.
const ErrNilOwnerKeyVerifier = internal.Error("owner-key verifier is nil")

// NewNeoKeyVerifier creates a new Neo owner key verifier and return a OwnerKeyVerifier interface.
func NewNeoKeyVerifier() OwnerKeyVerifier {
	return new(neoKeyVerifier)
}

// VerifyKey checks if the public key converts to owner ID.
//
// If passed OwnerKeyContainer is nil, ErrNilOwnerKeyContainer returns.
// If public key cannot be unmarshaled, service.ErrInvalidPublicKeyBytes returns.
// If public key is not converted to owner ID, service.ErrWrongOwner returns.
// With neo:morph adoption public key can be unrelated to owner ID. In this
// case VerifyKey should call NeoFS.ID smart-contract to check whether public
// key is bounded with owner ID. If there is no bound, then return
// service.ErrWrongOwner.
func (s neoKeyVerifier) VerifyKey(_ context.Context, src OwnerKeyContainer) error {
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
