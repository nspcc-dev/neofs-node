package objectcore

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// AddressWithAttributes groups object's address and its attributes.
type AddressWithAttributes struct {
	Address    oid.Address
	Type       object.Type
	Attributes []string
}
