package query

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

type Query interface {
	ToSearchFilters() objectSDK.SearchFilters
	Match(*object.Object, func(*objectSDK.ID))
}
