package objectcore

import (
	"errors"
	"strconv"

	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// ErrInvalidSearchQuery is returned when some object search query is invalid.
var ErrInvalidSearchQuery = errors.New("invalid search query")

// ErrNoExpiration means no expiration was set.
var ErrNoExpiration = errors.New("missing expiration epoch attribute")

// Expiration searches for expiration attribute in the object. Returns
// ErrNoExpiration if not found.
func Expiration(obj object.Object) (uint64, error) {
	for _, a := range obj.Attributes() {
		if a.Key() != object.AttributeExpirationEpoch {
			continue
		}

		return strconv.ParseUint(a.Value(), 10, 64)
	}

	return 0, ErrNoExpiration
}
