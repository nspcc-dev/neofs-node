package query

import (
	"encoding/hex"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
)

type Query struct {
	filters []*Filter
}

type matchType uint8

type Filter struct {
	matchType matchType

	key, val string
}

const (
	_ matchType = iota
	matchStringEqual
)

func New(filters ...*Filter) query.Query {
	return &Query{
		filters: filters,
	}
}

func idValue(id *objectSDK.ID) string {
	return hex.EncodeToString(id.ToV2().GetValue())
}

func NewIDEqualFilter(id *objectSDK.ID) *Filter {
	return NewFilterEqual(objectSDK.HdrSysNameID, idValue(id))
}

func cidValue(id *container.ID) string {
	return hex.EncodeToString(id.ToV2().GetValue())
}

func NewContainerIDEqualFilter(id *container.ID) *Filter {
	return NewFilterEqual(objectSDK.HdrSysNameCID, cidValue(id))
}

func ownerIDValue(id *owner.ID) string {
	return hex.EncodeToString(id.ToV2().GetValue())
}

func NewOwnerIDEqualFilter(id *owner.ID) *Filter {
	return NewFilterEqual(objectSDK.HdrSysNameOwnerID, ownerIDValue(id))
}

func NewFilterEqual(key, val string) *Filter {
	return &Filter{
		matchType: matchStringEqual,
		key:       key,
		val:       val,
	}
}

func (q *Query) Match(obj *object.Object) bool {
	for _, f := range q.filters {
		switch f.matchType {
		case matchStringEqual:
			if !headerEqual(obj, f.key, f.val) {
				return false
			}
		default:
			panic(fmt.Sprintf("unsupported match type %d", f.matchType))
		}
	}

	return true
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
	fs := make(objectSDK.SearchFilters, 0, len(q.filters))

	for i := range q.filters {
		fs.AddFilter(q.filters[i].key, q.filters[i].val, objectSDK.MatchStringEqual)
	}

	return fs
}
