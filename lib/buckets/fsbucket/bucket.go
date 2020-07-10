package fsbucket

import (
	"os"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
)

type (
	bucket struct {
		dir  string
		perm os.FileMode
	}

	treeBucket struct {
		dir  string
		perm os.FileMode

		depth        int
		prefixLength int
		sz           *atomic.Int64
	}
)

const (
	defaultDirectory   = "fsbucket"
	defaultPermissions = 0755
	defaultDepth       = 2
	defaultPrefixLen   = 2
)

const errShortKey = internal.Error("key is too short for tree fs bucket")

var _ core.Bucket = (*bucket)(nil)

func stringifyKey(key []byte) string {
	return base58.Encode(key)
}

func decodeKey(key string) []byte {
	k, err := base58.Decode(key)
	if err != nil {
		panic(err) // it can fail only for not base58 strings
	}

	return k
}

// NewBucket creates new in-memory bucket instance.
func NewBucket(name core.BucketType, v *viper.Viper) (core.Bucket, error) {
	var (
		key  = "storage." + string(name)
		dir  string
		perm os.FileMode

		prefixLen int
		depth     int
	)

	if dir = v.GetString(key + ".directory"); dir == "" {
		dir = defaultDirectory
	}

	if perm = os.FileMode(v.GetInt(key + ".permissions")); perm == 0 {
		perm = defaultPermissions
	}

	if depth = v.GetInt(key + ".depth"); depth <= 0 {
		depth = defaultDepth
	}

	if prefixLen = v.GetInt(key + ".prefix_len"); prefixLen <= 0 {
		prefixLen = defaultPrefixLen
	}

	if err := os.MkdirAll(dir, perm); err != nil {
		return nil, errors.Wrapf(err, "could not create bucket %s", string(name))
	}

	if v.GetBool(key + ".tree_enabled") {
		b := &treeBucket{
			dir:          dir,
			perm:         perm,
			depth:        depth,
			prefixLength: prefixLen,
		}
		b.sz = atomic.NewInt64(b.size())

		return b, nil
	}

	return &bucket{
		dir:  dir,
		perm: perm,
	}, nil
}
