package inmemory

import (
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/spf13/viper"
)

type (
	bucket struct {
		*sync.RWMutex
		items map[string][]byte
	}
)

const (
	defaultCapacity = 100
)

var (
	_ core.Bucket = (*bucket)(nil)

	// for in usage
	_ = NewBucket
)

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

func makeCopy(val []byte) []byte {
	tmp := make([]byte, len(val))
	copy(tmp, val)

	return tmp
}

// NewBucket creates new in-memory bucket instance.
func NewBucket(name core.BucketType, v *viper.Viper) core.Bucket {
	var capacity int
	if capacity = v.GetInt("storage." + string(name) + ".capacity"); capacity <= 0 {
		capacity = defaultCapacity
	}

	return &bucket{
		RWMutex: new(sync.RWMutex),
		items:   make(map[string][]byte, capacity),
	}
}
