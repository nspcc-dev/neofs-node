package meta

import (
	"encoding/binary"
	"encoding/hex"
	"strconv"

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

	matchers map[object.SearchMatchType]func(string, []byte, string) bool
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
		matchers: map[object.SearchMatchType]func(string, []byte, string) bool{
			object.MatchUnknown:     unknownMatcher,
			object.MatchStringEqual: stringEqualMatcher,
		},
	}
}

func (db *DB) Close() error {
	return db.boltDB.Close()
}

func stringEqualMatcher(key string, objVal []byte, filterVal string) bool {
	switch key {
	default:
		return string(objVal) == filterVal
	case v2object.FilterHeaderPayloadHash, v2object.FilterHeaderHomomorphicHash:
		return hex.EncodeToString(objVal) == filterVal
	case v2object.FilterHeaderCreationEpoch, v2object.FilterHeaderPayloadLength:
		return strconv.FormatUint(binary.LittleEndian.Uint64(objVal), 10) == filterVal
	}
}

func unknownMatcher(_ string, _ []byte, _ string) bool {
	return false
}

// bucketKeyHelper returns byte representation of val that is used as a key
// in boltDB. Useful for getting filter values from unique and list indexes.
func bucketKeyHelper(hdr string, val string) []byte {
	switch hdr {
	case v2object.FilterHeaderPayloadHash:
		v, err := hex.DecodeString(val)
		if err != nil {
			return nil
		}

		return v
	case v2object.FilterHeaderSplitID:
		s := object.NewSplitID()

		err := s.Parse(val)
		if err != nil {
			return nil
		}

		return s.ToV2()
	default:
		return []byte(val)
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
