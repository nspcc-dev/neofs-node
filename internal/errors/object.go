package errors

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectID is an object ID as error.
type ObjectID oid.ID

// Error returns string-encoded object ID.
func (x ObjectID) Error() string {
	return oid.ID(x).String()
}
