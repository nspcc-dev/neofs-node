package grpc

import (
	"errors"

	"github.com/nspcc-dev/neofs-api-go/service"
)

// ErrMissingKeySignPairs is returned by functions that expect
// a non-empty SignKeyPair slice, but received empty.
var ErrMissingKeySignPairs = errors.New("missing key-signature pairs")

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
