package query

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
)

// FIXME: this is a temporary solution for object fields filters

const keyParentField = "Object.Header.Split.WithChildren"

const keyNoChildrenField = "Object.Header.Split.NoChildren"

const keyParentIDField = "Object.Header.Split.Parent"

func NewEmptyChildrenFilter() *Filter {
	return NewFilterEqual(keyNoChildrenField, "")
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

func NewLinkingQuery(par *object.ID) query.Query {
	return New(
		NewParentIDFilter(par),
		NewFilterEqual(keyParentField, ""),
	)
}
