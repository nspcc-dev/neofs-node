package ec

import (
	"strconv"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrParts is error carrying ID set of EC parts.
type ErrParts []oid.ID

// Error implements [error].
func (x ErrParts) Error() string {
	return strconv.Itoa(len(x)) + " EC parts"
}
