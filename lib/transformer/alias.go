package transformer

import (
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/storagegroup"
)

type (
	// Object is a type alias of
	// Object from object package of neofs-api-go.
	Object = object.Object

	// ObjectID is a type alias of
	// ObjectID from refs package of neofs-api-go.
	ObjectID = refs.ObjectID

	// CID is a type alias of
	// CID from refs package of neofs-api-go.
	CID = refs.CID

	// StorageGroup is a type alias of
	// StorageGroup from storagegroup package of neofs-api-go.
	StorageGroup = storagegroup.StorageGroup
)
