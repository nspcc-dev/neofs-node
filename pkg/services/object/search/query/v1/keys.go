package query

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
)

// FIXME: this is a temporary solution for object fields filters

func NewRightChildQuery(par *object.ID) query.Query {
	q := &Query{
		filters: make(object.SearchFilters, 0, 2),
	}

	q.filters.AddFilter(v2object.FilterHeaderParent, idValue(par), object.MatchStringEqual)
	q.filters.AddFilter(v2object.FilterPropertyChildfree, v2object.BooleanPropertyValueTrue, object.MatchStringEqual)

	return q
}

func NewLinkingQuery(par *object.ID) query.Query {
	q := &Query{
		filters: make(object.SearchFilters, 0, 2),
	}

	q.filters.AddFilter(v2object.FilterHeaderParent, idValue(par), object.MatchStringEqual)
	q.filters.AddFilter(v2object.FilterPropertyChildfree, v2object.BooleanPropertyValueFalse, object.MatchStringEqual)

	return q
}
