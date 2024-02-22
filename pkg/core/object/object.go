package object

import (
	"errors"

	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrInvalidSearchQuery is returned when some object search query is invalid.
var ErrInvalidSearchQuery = errors.New("invalid search query")

// AddressOf returns the address of the object.
func AddressOf(obj *object.Object) oid.Address {
	var addr oid.Address

	id, ok := obj.ID()
	if ok {
		addr.SetObject(id)
	}

	cnr, ok := obj.ContainerID()
	if ok {
		addr.SetContainer(cnr)
	}

	return addr
}
