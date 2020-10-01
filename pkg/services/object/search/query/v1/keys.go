package query

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
)

// FIXME: this is a temporary solution for object fields filters

const keyParentField = "Object.Header.Split.WithChildren"

const keyNoChildrenField = "Object.Header.Split.NoChildren"

const keyParentIDField = "Object.Header.Split.Parent"

func NewRightChildQuery(par *object.ID) query.Query {
	q := &Query{
		filters: make(object.SearchFilters, 0, 2),
	}

	q.filters.AddFilter(keyParentIDField, idValue(par), object.MatchStringEqual)
	q.filters.AddFilter(keyNoChildrenField, "", object.MatchStringEqual)

	return q
}

func NewLinkingQuery(par *object.ID) query.Query {
	q := &Query{
		filters: make(object.SearchFilters, 0, 2),
	}

	q.filters.AddFilter(keyParentIDField, idValue(par), object.MatchStringEqual)
	q.filters.AddFilter(keyParentField, "", object.MatchStringEqual)

	return q
}
