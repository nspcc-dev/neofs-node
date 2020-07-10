package core

import (
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/internal"
)

// ErrMissingKeySignPairs is returned by functions that expect
// a non-empty SignKeyPair slice, but received empty.
const ErrMissingKeySignPairs = internal.Error("missing key-signature pairs")

// VerifyRequestWithSignatures checks if request has signatures and all of them are valid.
//
// Returns ErrMissingKeySignPairs if request does not have signatures.
// Otherwise, behaves like service.VerifyRequestData.
func VerifyRequestWithSignatures(req service.RequestVerifyData) error {
	if len(req.GetSignKeyPairs()) == 0 {
		return ErrMissingKeySignPairs
	}

	return service.VerifyRequestData(req)
}
