package object

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// AddressWithType groups object address with its NeoFS
// object type.
type AddressWithType struct {
	Address oid.Address
	Type    object.Type
}
