package meta

import (
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// Search selects up to count container's objects from the given container
// matching the specified filters.
func (m *Meta) Search(cID cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	return m.metabase.Search(cID, fs, attrs, cursor, count)
}
