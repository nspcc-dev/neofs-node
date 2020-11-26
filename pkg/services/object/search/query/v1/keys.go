package query

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
)

// FIXME: this is a temporary solution for object fields filters

// fixme: remove it, it is broken now
func NewRightChildQuery(par *object.ID) query.Query {
	q := &Query{
		filters: make(object.SearchFilters, 0, 2),
	}

	q.filters.AddFilter(v2object.FilterHeaderParent, idValue(par), object.MatchStringEqual)

	return q
}

// fixme: remove it, it is broken now
func NewLinkingQuery(par *object.ID) query.Query {
	q := &Query{
		filters: make(object.SearchFilters, 0, 2),
	}

	q.filters.AddFilter(v2object.FilterHeaderParent, idValue(par), object.MatchStringEqual)

	return q
}
