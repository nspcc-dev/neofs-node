package main

import (
	"github.com/nspcc-dev/neofs-node/lib/buckets/boltdb"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var _ = PrepareBucket

// PrepareBucket is interface method for bucket.
func PrepareBucket(name core.BucketType, v *viper.Viper) (db core.Bucket, err error) {
	var opts boltdb.Options

	if opts, err = boltdb.NewOptions("storage."+name, v); err != nil {
		err = errors.Wrapf(err, "%q: could not prepare options", name)
		return
	} else if db, err = boltdb.NewBucket(&opts); err != nil {
		err = errors.Wrapf(err, "%q: could not prepare bucket", name)
		return
	}

	return
}
