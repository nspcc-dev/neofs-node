package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// DB represents local metabase of storage node.
type DB struct {
	path string

	*cfg

	matchers map[object.SearchMatchType]func(string, string, string) bool
}

// Option is an option of DB constructor.
type Option func(*cfg)

type cfg struct {
	boltDB *bbolt.DB

	log *logger.Logger
}

func defaultCfg() *cfg {
	return &cfg{
		log: zap.L(),
	}
}

// NewDB creates, initializes and returns DB instance.
func NewDB(opts ...Option) *DB {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &DB{
		path: c.boltDB.Path(),
		cfg:  c,
		matchers: map[object.SearchMatchType]func(string, string, string) bool{
			object.MatchStringEqual: stringEqualMatcher,
		},
	}
}

func (db *DB) Close() error {
	return db.boltDB.Close()
}

// Path returns the path to meta database.
func (db *DB) Path() string {
	return db.path
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

// FromBoltDB returns option to construct DB from BoltDB instance.
func FromBoltDB(db *bbolt.DB) Option {
	return func(c *cfg) {
		c.boltDB = db
	}
}

// WithLogger returns option to set logger of DB.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}
