package node

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket/boltdb"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket/fsbucket"
	"github.com/spf13/viper"
)

type Buckets map[string]bucket.Bucket

const (
	fsBucket   = "fsbucket"
	boltBucket = "bolt"
)

func newBuckets(v *viper.Viper) (Buckets, error) {
	var (
		err      error
		mBuckets = make(Buckets)
	)

	if mBuckets[fsBucket], err = fsbucket.NewBucket(v); err != nil {
		return nil, err
	}

	boltOpts, err := boltdb.NewOptions(v)
	if err != nil {
		return nil, err
	} else if mBuckets[boltBucket], err = boltdb.NewBucket(&boltOpts); err != nil {
		return nil, err
	}

	return mBuckets, nil
}
