package meta

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mr-tron/base58"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type matcher struct {
	matchSlow   func(string, []byte, string) bool
	matchBucket func(*bbolt.Bucket, string, string, func([]byte, []byte) error) error
}

// EpochState is an interface that provides access to the
// current epoch number.
type EpochState interface {
	// CurrentEpoch must return current epoch height.
	CurrentEpoch() uint64
}

// DB represents local metabase of storage node.
type DB struct {
	*cfg

	modeMtx sync.RWMutex
	mode    mode.Mode

	matchers map[object.SearchMatchType]matcher

	boltDB *bbolt.DB

	initialized bool
}

// Option is an option of DB constructor.
type Option func(*cfg)

type cfg struct {
	boltOptions *bbolt.Options // optional

	boltBatchSize  int
	boltBatchDelay time.Duration

	info Info

	log *logger.Logger

	epochState EpochState
}

func defaultCfg() *cfg {
	return &cfg{
		info: Info{
			Permission: os.ModePerm, // 0777
		},
		boltBatchDelay: bbolt.DefaultMaxBatchDelay,
		boltBatchSize:  bbolt.DefaultMaxBatchSize,
		log:            zap.L(),
	}
}

// New creates and returns new Metabase instance.
func New(opts ...Option) *DB {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	if c.epochState == nil {
		panic("metabase: epoch state is not specified")
	}

	return &DB{
		cfg: c,
		matchers: map[object.SearchMatchType]matcher{
			object.MatchUnknown: {
				matchSlow:   unknownMatcher,
				matchBucket: unknownMatcherBucket,
			},
			object.MatchStringEqual: {
				matchSlow:   stringEqualMatcher,
				matchBucket: stringEqualMatcherBucket,
			},
			object.MatchStringNotEqual: {
				matchSlow:   stringNotEqualMatcher,
				matchBucket: stringNotEqualMatcherBucket,
			},
			object.MatchCommonPrefix: {
				matchSlow:   stringCommonPrefixMatcher,
				matchBucket: stringCommonPrefixMatcherBucket,
			},
		},
	}
}

func stringifyValue(key string, objVal []byte) string {
	switch key {
	default:
		return string(objVal)
	case v2object.FilterHeaderObjectID, v2object.FilterHeaderContainerID, v2object.FilterHeaderParent:
		return base58.Encode(objVal)
	case v2object.FilterHeaderPayloadHash, v2object.FilterHeaderHomomorphicHash:
		return hex.EncodeToString(objVal)
	case v2object.FilterHeaderCreationEpoch, v2object.FilterHeaderPayloadLength:
		return strconv.FormatUint(binary.LittleEndian.Uint64(objVal), 10)
	}
}

// fromHexChar converts a hex character into its value and a success flag.
func fromHexChar(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}

	return 0, false
}

// destringifyValue is the reverse operation for stringify value.
// The last return value returns true if the filter CAN match any value.
// The second return value is true iff prefix is true and the filter value is considered
// a hex-encoded string. In this case only the first (highest) bits of the last byte should be checked.
func destringifyValue(key, value string, prefix bool) ([]byte, bool, bool) {
	switch key {
	default:
		return []byte(value), false, true
	case v2object.FilterHeaderObjectID, v2object.FilterHeaderContainerID, v2object.FilterHeaderParent:
		v, err := base58.Decode(value)
		return v, false, err == nil
	case v2object.FilterHeaderPayloadHash, v2object.FilterHeaderHomomorphicHash:
		v, err := hex.DecodeString(value)
		if err != nil {
			if !prefix || len(value)%2 == 0 {
				return v, false, false
			}
			// To match the old behaviour we need to process odd length hex strings, such as 'abc'
			last, ok := fromHexChar(value[len(value)-1])
			if !ok {
				return v, false, false
			}

			v := make([]byte, hex.DecodedLen(len(value)-1)+1)
			_, err := hex.Decode(v, []byte(value[:len(value)-1]))
			if err != nil {
				return nil, false, false
			}
			v[len(v)-1] = last

			return v, true, true
		}
		return v, false, err == nil
	case v2object.FilterHeaderCreationEpoch, v2object.FilterHeaderPayloadLength:
		u, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, false, false
		}
		raw := make([]byte, 8)
		binary.LittleEndian.PutUint64(raw, u)
		return raw, false, true
	}
}

func stringEqualMatcher(key string, objVal []byte, filterVal string) bool {
	return stringifyValue(key, objVal) == filterVal
}

func stringEqualMatcherBucket(b *bbolt.Bucket, fKey string, fValue string, f func([]byte, []byte) error) error {
	// Ignore the second return value because we check for strict equality.
	val, _, ok := destringifyValue(fKey, fValue, false)
	if !ok {
		return nil
	}
	if data := b.Get(val); data != nil {
		return f(val, data)
	}
	if b.Bucket(val) != nil {
		return f(val, nil)
	}
	return nil
}

func stringNotEqualMatcher(key string, objVal []byte, filterVal string) bool {
	return stringifyValue(key, objVal) != filterVal
}

func stringNotEqualMatcherBucket(b *bbolt.Bucket, fKey string, fValue string, f func([]byte, []byte) error) error {
	// Ignore the second return value because we check for strict inequality.
	val, _, ok := destringifyValue(fKey, fValue, false)
	return b.ForEach(func(k, v []byte) error {
		if !ok || !bytes.Equal(val, k) {
			return f(k, v)
		}
		return nil
	})
}

func stringCommonPrefixMatcher(key string, objVal []byte, filterVal string) bool {
	return strings.HasPrefix(stringifyValue(key, objVal), filterVal)
}

func stringCommonPrefixMatcherBucket(b *bbolt.Bucket, fKey string, fVal string, f func([]byte, []byte) error) error {
	val, checkLast, ok := destringifyValue(fKey, fVal, true)
	if !ok {
		return nil
	}

	prefix := val
	if checkLast {
		prefix = val[:len(val)-1]
	}

	if len(val) == 0 {
		// empty common prefix, all the objects
		// satisfy that filter
		return b.ForEach(f)
	}

	c := b.Cursor()
	for k, v := c.Seek(val); bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if checkLast && (len(k) == len(prefix) || k[len(prefix)]>>4 != val[len(val)-1]) {
			// If the last byte doesn't match, this means the prefix does no longer match,
			// so we need to break here.
			break
		}
		if err := f(k, v); err != nil {
			return err
		}
	}
	return nil
}

func unknownMatcher(_ string, _ []byte, _ string) bool {
	return false
}

func unknownMatcherBucket(_ *bbolt.Bucket, _ string, _ string, _ func([]byte, []byte) error) error {
	return nil
}

// bucketKeyHelper returns byte representation of val that is used as a key
// in boltDB. Useful for getting filter values from unique and list indexes.
func bucketKeyHelper(hdr string, val string) []byte {
	switch hdr {
	case v2object.FilterHeaderParent:
		v, err := base58.Decode(val)
		if err != nil {
			return nil
		}
		return v
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

// SetLogger sets logger. It is used after the shard ID was generated to use it in logs.
func (db *DB) SetLogger(l *zap.Logger) {
	db.log = l
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
func WithPermissions(perm fs.FileMode) Option {
	return func(c *cfg) {
		c.info.Permission = perm
	}
}

// WithMaxBatchSize returns option to specify maximum concurrent operations
// to be processed in a single transactions.
// This option is missing from `bbolt.Options` but is set right after DB is open.
func WithMaxBatchSize(s int) Option {
	return func(c *cfg) {
		if s != 0 {
			c.boltBatchSize = s
		}
	}
}

// WithMaxBatchDelay returns option to specify maximum time to wait before
// the batch of concurrent transactions is processed.
// This option is missing from `bbolt.Options` but is set right after DB is open.
func WithMaxBatchDelay(d time.Duration) Option {
	return func(c *cfg) {
		if d != 0 {
			c.boltBatchDelay = d
		}
	}
}

// WithEpochState return option to specify a source of current epoch height.
func WithEpochState(s EpochState) Option {
	return func(c *cfg) {
		c.epochState = s
	}
}
