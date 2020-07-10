package localstore

import (
	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
)

// CID is a type alias of
// CID from refs package of neofs-api-go.
type CID = refs.CID

// SGID is a type alias of
// SGID from refs package of neofs-api-go.
type SGID = refs.ObjectID

// Header is a type alias of
// Header from object package of neofs-api-go.
type Header = object.Header

// Object is a type alias of
// Object from object package of neofs-api-go.
type Object = object.Object

// ObjectID is a type alias of
// ObjectID from refs package of neofs-api-go.
type ObjectID = refs.ObjectID

// Address is a type alias of
// Address from refs package of neofs-api-go.
type Address = refs.Address

// Hash is a type alias of
// Hash from hash package of neofs-api-go.
type Hash = hash.Hash
