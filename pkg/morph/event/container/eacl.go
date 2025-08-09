package container

import (
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
)

// SetEACL represents structure of notification about
// modified eACL table coming from NeoFS Container contract.
type SetEACL struct {
	table     []byte
	signature []byte
	publicKey []byte
	token     []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (SetEACL) MorphEvent() {}

// Table returns eACL table in a binary NeoFS API format.
func (x SetEACL) Table() []byte {
	return x.table
}

// Signature returns signature of the binary table.
func (x SetEACL) Signature() []byte {
	return x.signature
}

// PublicKey returns public keys of container
// owner in a binary format.
func (x SetEACL) PublicKey() []byte {
	return x.publicKey
}

// SessionToken returns binary token of the session
// within which the eACL was set.
func (x SetEACL) SessionToken() []byte {
	return x.token
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (x SetEACL) NotaryRequest() *payload.P2PNotaryRequest {
	return x.notaryRequest
}
