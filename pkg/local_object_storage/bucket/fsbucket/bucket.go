package fsbucket

import (
	"os"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
)

type (
	Bucket struct {
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

const name = "fsbucket"

const (
	defaultDirectory   = "fsbucket"
	defaultPermissions = 0755
	defaultDepth       = 2
	defaultPrefixLen   = 2
)

var errShortKey = errors.New("key is too short for tree fs bucket")

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

// NewBucket creates new file system bucket instance.
func NewBucket(v *viper.Viper) (bucket.Bucket, error) {
	var (
		dir  string
		perm os.FileMode

		prefixLen int
		depth     int
	)

	if dir = v.GetString(name + ".directory"); dir == "" {
		dir = defaultDirectory
	}

	if perm = os.FileMode(v.GetInt(name + ".permissions")); perm == 0 {
		perm = defaultPermissions
	}

	if depth = v.GetInt(name + ".depth"); depth <= 0 {
		depth = defaultDepth
	}

	if prefixLen = v.GetInt(name + ".prefix_len"); prefixLen <= 0 {
		prefixLen = defaultPrefixLen
	}

	if err := os.MkdirAll(dir, perm); err != nil {
		return nil, errors.Wrapf(err, "could not create bucket %s", name)
	}

	if v.GetBool(name + ".tree_enabled") {
		b := &treeBucket{
			dir:          dir,
			perm:         perm,
			depth:        depth,
			prefixLength: prefixLen,
		}
		b.sz = atomic.NewInt64(b.size())

		return b, nil
	}

	return &Bucket{
		dir:  dir,
		perm: perm,
	}, nil
}
