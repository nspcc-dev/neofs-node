package query

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
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
	return hex.EncodeToString(id.ToV2().GetValue())
}

func cidValue(id *container.ID) string {
	return hex.EncodeToString(id.ToV2().GetValue())
}

func ownerIDValue(id *owner.ID) string {
	return hex.EncodeToString(id.ToV2().GetValue())
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
				case objectSDK.KeyRoot:
					match = (q.filters[i].Value() == objectSDK.ValRoot) == (!obj.HasParent() &&
						obj.GetType() == objectSDK.TypeRegular)
				case objectSDK.KeyLeaf:
					match = (q.filters[i].Value() == objectSDK.ValLeaf) == (par == nil)
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
	case objectSDK.HdrSysNameID:
		return value == idValue(obj.GetID())
	case objectSDK.HdrSysNameCID:
		return value == cidValue(obj.GetContainerID())
	case objectSDK.HdrSysNameOwnerID:
		return value == ownerIDValue(obj.GetOwnerID())
	case keyNoChildrenField:
		return len(obj.GetChildren()) == 0
	case keyParentIDField:
		return idValue(obj.GetParentID()) == value
	case keyParentField:
		return len(obj.GetChildren()) > 0
		// TODO: add other headers
	}
}

func (q *Query) ToSearchFilters() objectSDK.SearchFilters {
	return q.filters
}
