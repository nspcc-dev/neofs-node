package query

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
)

type Query interface {
	ToSearchFilters() objectSDK.SearchFilters
}
