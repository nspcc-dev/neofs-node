package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"go.etcd.io/bbolt"
)

// DB represents local metabase of storage node.
type DB struct {
	boltDB *bbolt.DB

	matchers map[object.SearchMatchType]func(string, string, string) bool
}

// NewDB creates, initializes and returns DB instance.
func NewDB(boltDB *bbolt.DB) *DB {
	return &DB{
		boltDB: boltDB,
		matchers: map[object.SearchMatchType]func(string, string, string) bool{
			object.MatchStringEqual: stringEqualMatcher,
		},
	}
}

func (db *DB) Close() error {
	return db.boltDB.Close()
}

func stringEqualMatcher(key, objVal, filterVal string) bool {
	switch key {
	default:
		return objVal == filterVal
	case
		v2object.FilterPropertyRoot,
		v2object.FilterPropertyChildfree,
		v2object.FilterPropertyLeaf:
		return (filterVal == v2object.BooleanPropertyValueTrue) == (objVal == v2object.BooleanPropertyValueTrue)
	}
}
