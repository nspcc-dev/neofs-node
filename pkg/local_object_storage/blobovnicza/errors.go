package blobovnicza

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// IsErrNotFound checks if the error returned by Blobovnicza Get/Delete method
// corresponds to the missing object.
func IsErrNotFound(err error) bool {
	return errors.As(err, new(apistatus.ObjectNotFound))
}
