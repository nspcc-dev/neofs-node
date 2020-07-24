package object

import (
	"github.com/nspcc-dev/neofs-api-go/refs"
)

// ID represents the object identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/refs.ObjectID.
// FIXME: object ID should be defined in core package.
type ID = refs.ObjectID

// Address represents NeoFS Object address.
// Acts as a reference to the object.
type Address struct {
	cid CID

	id ID
}

// CID return the identifier of the container
// that the object belongs to.
func (a Address) CID() CID {
	return a.cid
}

// SetCID sets the identifier of the container
// that the object belongs to.
func (a *Address) SetCID(v CID) {
	a.cid = v
}

// ID returns the unique identifier of the
// object in container.
func (a Address) ID() ID {
	return a.id
}

// SetID sets the unique identifier of the
// object in container.
func (a *Address) SetID(v ID) {
	a.id = v
}

// AddressFromObject returns an address based
// on the object's header.
//
// Returns nil on nil object.
func AddressFromObject(o *Object) *Address {
	if o == nil {
		return nil
	}

	a := new(Address)

	a.SetCID(o.CID())
	a.SetID(o.ID())

	return a
}
