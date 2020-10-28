package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
)

// DB represents local metabase of storage node.
type DB struct {
	boltDB *bbolt.DB

	matchers map[object.SearchMatchType]func(string, string) bool
}

// NewDB creates, initializes and returns DB instance.
func NewDB(boltDB *bbolt.DB) *DB {
	return &DB{
		boltDB: boltDB,
		matchers: map[object.SearchMatchType]func(string, string) bool{
			object.MatchStringEqual: func(s string, s2 string) bool {
				return s == s2
			},
		},
	}
}
