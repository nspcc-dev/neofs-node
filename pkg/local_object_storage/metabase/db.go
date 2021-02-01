package meta

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// DB represents local metabase of storage node.
type DB struct {
	*cfg

	matchers map[object.SearchMatchType]func(string, []byte, string) bool

	boltDB *bbolt.DB
}

// Option is an option of DB constructor.
type Option func(*cfg)

type cfg struct {
	boltOptions *bbolt.Options // optional

	info Info

	log *logger.Logger
}

func defaultCfg() *cfg {
	return &cfg{
		info: Info{
			Permission: os.ModePerm, // 0777
		},

		log: zap.L(),
	}
}

// New creates and returns new Metabase instance.
func New(opts ...Option) *DB {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &DB{
		cfg: c,
		matchers: map[object.SearchMatchType]func(string, []byte, string) bool{
			object.MatchUnknown:        unknownMatcher,
			object.MatchStringEqual:    stringEqualMatcher,
			object.MatchStringNotEqual: stringNotEqualMatcher,
		},
	}
}

func stringifyValue(key string, objVal []byte) string {
	switch key {
	default:
		return string(objVal)
	case v2object.FilterHeaderPayloadHash, v2object.FilterHeaderHomomorphicHash:
		return hex.EncodeToString(objVal)
	case v2object.FilterHeaderCreationEpoch, v2object.FilterHeaderPayloadLength:
		return strconv.FormatUint(binary.LittleEndian.Uint64(objVal), 10)
	}
}

func stringEqualMatcher(key string, objVal []byte, filterVal string) bool {
	return stringifyValue(key, objVal) == filterVal
}

func stringNotEqualMatcher(key string, objVal []byte, filterVal string) bool {
	return stringifyValue(key, objVal) != filterVal
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

// WithLogger returns option to set logger of DB.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}

// WithBoltDBOptions returns option to specify BoltDB options.
func WithBoltDBOptions(opts *bbolt.Options) Option {
	return func(c *cfg) {
		c.boltOptions = opts
	}
}

// WithPath returns option to set system path to Metabase.
func WithPath(path string) Option {
	return func(c *cfg) {
		c.info.Path = path
	}
}

// WithPermissions returns option to specify permission bits
// of Metabase system path.
func WithPermissions(perm os.FileMode) Option {
	return func(c *cfg) {
		c.info.Permission = perm
	}
}
