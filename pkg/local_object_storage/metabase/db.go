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
	info Info

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
		info: Info{
			Path: c.boltDB.Path(),
		},
		cfg: c,
		matchers: map[object.SearchMatchType]func(string, string, string) bool{
			object.MatchUnknown:     unknownMatcher,
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
	case v2object.FilterPropertyChildfree:
		return (filterVal == v2object.BooleanPropertyValueTrue) == (objVal == v2object.BooleanPropertyValueTrue)
	case v2object.FilterPropertyPhy, v2object.FilterPropertyRoot:
		return true
	}
}

func unknownMatcher(key, _, _ string) bool {
	switch key {
	default:
		return false
	case v2object.FilterPropertyPhy, v2object.FilterPropertyRoot:
		return true
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
