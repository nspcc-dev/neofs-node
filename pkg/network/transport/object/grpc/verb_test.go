package object

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/stretchr/testify/require"
)

func TestAllowedSpawn(t *testing.T) {
	items := []struct {
		to   object.RequestType
		ok   []Verb
		fail []Verb
	}{
		{ // Put
			to: object.RequestPut,
			ok: []Verb{
				service.Token_Info_Put,
				service.Token_Info_Delete,
			},
			fail: []Verb{
				service.Token_Info_Get,
				service.Token_Info_Head,
				service.Token_Info_Range,
				service.Token_Info_RangeHash,
			},
		},
		{ // Get
			to: object.RequestGet,
			ok: []Verb{
				service.Token_Info_Get,
			},
			fail: []Verb{
				service.Token_Info_Put,
				service.Token_Info_Delete,
				service.Token_Info_RangeHash,
				service.Token_Info_Head,
				service.Token_Info_Search,
				service.Token_Info_Range,
			},
		},
		{ // Head
			to: object.RequestHead,
			ok: []Verb{
				service.Token_Info_Head,
				service.Token_Info_Put,
				service.Token_Info_Range,
				service.Token_Info_Get,
				service.Token_Info_RangeHash,
			},
			fail: []Verb{
				service.Token_Info_Search,
				service.Token_Info_Delete,
			},
		},
		{ // Delete
			to: object.RequestDelete,
			ok: []Verb{
				service.Token_Info_Delete,
			},
			fail: []Verb{
				service.Token_Info_Get,
				service.Token_Info_Head,
				service.Token_Info_Range,
				service.Token_Info_RangeHash,
				service.Token_Info_Put,
				service.Token_Info_Search,
			},
		},
		{ // Search
			to: object.RequestSearch,
			ok: []Verb{
				service.Token_Info_Put,
				service.Token_Info_Get,
				service.Token_Info_Head,
				service.Token_Info_Delete,
				service.Token_Info_Range,
				service.Token_Info_RangeHash,
				service.Token_Info_Search,
			},
			fail: []Verb{},
		},
		{ // Range
			to: object.RequestRange,
			ok: []Verb{
				service.Token_Info_Get,
				service.Token_Info_Range,
			},
			fail: []Verb{
				service.Token_Info_Put,
				service.Token_Info_Delete,
				service.Token_Info_RangeHash,
				service.Token_Info_Head,
				service.Token_Info_Search,
			},
		},
		{ // RangeHash
			to: object.RequestRangeHash,
			ok: []Verb{
				service.Token_Info_RangeHash,
			},
			fail: []Verb{
				service.Token_Info_Put,
				service.Token_Info_Get,
				service.Token_Info_Delete,
				service.Token_Info_Range,
				service.Token_Info_Head,
				service.Token_Info_Search,
			},
		},
	}

	for _, item := range items {
		for _, from := range item.ok {
			require.True(t, allowedSpawn(from, item.to))
		}

		for _, from := range item.fail {
			require.False(t, allowedSpawn(from, item.to))
		}
	}
}
