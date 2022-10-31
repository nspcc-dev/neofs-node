package object

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
)

// ErrObjectIsExpired is returned when the requested object's
// epoch is less than the current one. Such objects are considered
// as removed and should not be returned from the Storage Engine.
var ErrObjectIsExpired = logicerr.New("object is expired")
