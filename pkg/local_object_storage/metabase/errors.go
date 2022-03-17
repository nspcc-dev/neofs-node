package meta

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// IsErrRemoved checks if error returned by Shard Exists/Get/Put method
// corresponds to removed object.
func IsErrRemoved(err error) bool {
	return errors.As(err, new(apistatus.ObjectAlreadyRemoved))
}
