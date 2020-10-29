package query

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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

func (q *Query) Match(obj *object.Object, handler func(*objectSDK.ID)) {
	for par := (*object.Object)(nil); obj != nil; par, obj = obj, obj.GetParent() {
		match := true

		for i := 0; match && i < len(q.filters); i++ {
			switch typ := q.filters[i].Operation(); typ {
			default:
				match = false
			case objectSDK.MatchStringEqual:
				switch key := q.filters[i].Header(); key {
				default:
					match = headerEqual(obj, key, q.filters[i].Value())
				case v2object.FilterPropertyRoot:
					match = (q.filters[i].Value() == v2object.BooleanPropertyValueTrue) == (!obj.HasParent() &&
						obj.GetType() == objectSDK.TypeRegular)
				case v2object.FilterPropertyLeaf:
					match = (q.filters[i].Value() == v2object.BooleanPropertyValueTrue) == (par == nil)
				}
			}
		}

		if match {
			handler(obj.GetID())
		}
	}
}

func headerEqual(obj *object.Object, key, value string) bool {
	switch key {
	default:
		for _, attr := range obj.GetAttributes() {
			if attr.GetKey() == key && attr.GetValue() == value {
				return true
			}
		}

		return false
	case v2object.FilterHeaderContainerID:
		return value == cidValue(obj.GetContainerID())
	case v2object.FilterHeaderOwnerID:
		return value == ownerIDValue(obj.GetOwnerID())
	case v2object.FilterPropertyChildfree:
		return (value == v2object.BooleanPropertyValueTrue) == (len(obj.GetChildren()) == 0)
	case v2object.FilterHeaderParent:
		return idValue(obj.GetParentID()) == value
		// TODO: add other headers
	}
}

func (q *Query) ToSearchFilters() objectSDK.SearchFilters {
	return q.filters
}
