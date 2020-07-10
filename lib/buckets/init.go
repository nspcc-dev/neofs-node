package buckets

import (
	"plugin"
	"strings"

	"github.com/nspcc-dev/neofs-node/lib/buckets/boltdb"
	"github.com/nspcc-dev/neofs-node/lib/buckets/fsbucket"
	"github.com/nspcc-dev/neofs-node/lib/buckets/inmemory"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	// BoltDBBucket is a name of BoltDB bucket.
	BoltDBBucket = "boltdb"

	// InMemoryBucket is a name RAM bucket.
	InMemoryBucket = "in-memory"

	// FileSystemBucket is a name of file system bucket.
	FileSystemBucket = "fsbucket"

	bucketSymbol = "PrepareBucket"
)

// NewBucket is a bucket's constructor.
func NewBucket(name core.BucketType, l *zap.Logger, v *viper.Viper) (core.Bucket, error) {
	bucket := v.GetString("storage." + string(name) + ".bucket")

	l.Info("initialize bucket",
		zap.String("name", string(name)),
		zap.String("bucket", bucket))

	switch strings.ToLower(bucket) {
	case FileSystemBucket:
		return fsbucket.NewBucket(name, v)

	case InMemoryBucket:
		return inmemory.NewBucket(name, v), nil

	case BoltDBBucket:
		opts, err := boltdb.NewOptions("storage."+name, v)
		if err != nil {
			return nil, err
		}

		return boltdb.NewBucket(&opts)
	default:
		instance, err := plugin.Open(bucket)
		if err != nil {
			return nil, errors.Wrapf(err, "could not load bucket: `%s`", bucket)
		}

		sym, err := instance.Lookup(bucketSymbol)
		if err != nil {
			return nil, errors.Wrapf(err, "could not find bucket signature: `%s`", bucket)
		}

		return sym.(func(core.BucketType, *viper.Viper) (core.Bucket, error))(name, v)
	}
}
