package objectcore

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// AddressWithAttributes groups object's address, its attributes and the IDs of
// local shards that store the object.
type AddressWithAttributes struct {
	Address    oid.Address
	Type       object.Type
	Attributes []string
	ShardIDs   []string
}
