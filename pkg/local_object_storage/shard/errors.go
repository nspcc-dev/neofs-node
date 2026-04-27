package shard

import (
	"errors"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
)

// IsErrObjectExpired checks if an error returned by Shard corresponds to
// expired object.
func IsErrObjectExpired(err error) bool {
	return errors.Is(err, meta.ErrObjectIsExpired)
}
