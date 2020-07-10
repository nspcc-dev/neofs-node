package meta

import (
	"github.com/nspcc-dev/neofs-api-go/object"
)

type (
	// Iterator is an interface of the iterator over object storage.
	Iterator interface {
		Iterate(IterateFunc) error
	}

	// IterateFunc is a function that checks whether an object matches a specific criterion.
	IterateFunc func(*object.Object) error
)
