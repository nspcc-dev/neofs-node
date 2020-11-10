package query

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
)

type Query struct {
	filters objectSDK.SearchFilters
}

func New(filters objectSDK.SearchFilters) query.Query {
	return &Query{
		filters: filters,
	}
}

func idValue(id *objectSDK.ID) string {
	return id.String()
}

func cidValue(id *container.ID) string {
	return id.String()
}

func ownerIDValue(id *owner.ID) string {
	return id.String()
}

func (q *Query) ToSearchFilters() objectSDK.SearchFilters {
	return q.filters
}
