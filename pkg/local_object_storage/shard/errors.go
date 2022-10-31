package shard

import (
	"errors"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// IsErrNotFound checks if error returned by Shard Get/Head/GetRange method
// corresponds to missing object.
func IsErrNotFound(err error) bool {
	return errors.As(err, new(apistatus.ObjectNotFound))
}

// IsErrRemoved checks if error returned by Shard Exists/Get/Head/GetRange method
// corresponds to removed object.
func IsErrRemoved(err error) bool {
	return errors.As(err, new(apistatus.ObjectAlreadyRemoved))
}

// IsErrOutOfRange checks if an error returned by Shard GetRange method
// corresponds to exceeding the object bounds.
func IsErrOutOfRange(err error) bool {
	return errors.As(err, new(apistatus.ObjectOutOfRange))
}

// IsErrObjectExpired checks if an error returned by Shard corresponds to
// expired object.
func IsErrObjectExpired(err error) bool {
	return errors.Is(err, meta.ErrObjectIsExpired)
}
