package query

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
)

// FIXME: this is a temporary solution for object fields filters

const keyChildrenField = "Object.Header.Split.Children"

const keyParentIDField = "Object.Header.Split.Parent"

func NewEmptyChildrenFilter() *Filter {
	return NewFilterEqual(keyChildrenField, "")
}

func NewParentIDFilter(par *object.ID) *Filter {
	return NewFilterEqual(keyParentIDField, idValue(par))
}

func NewRightChildQuery(par *object.ID) query.Query {
	return New(
		NewParentIDFilter(par),
		NewEmptyChildrenFilter(),
	)
}
