package writecache

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// IsErrNotFound checks if error returned by Cache Get/Head/Delete method
// corresponds to missing object.
func IsErrNotFound(err error) bool {
	return errors.As(err, new(apistatus.ObjectNotFound))
}
