package blobovnicza

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// IsErrNotFound checks if error returned by Blobovnicza Get/Delete method
// corresponds to missing object.
func IsErrNotFound(err error) bool {
	return errors.As(err, new(apistatus.ObjectNotFound))
}
