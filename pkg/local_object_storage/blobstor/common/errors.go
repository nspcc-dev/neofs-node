package common

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
)

// ErrReadOnly MUST be returned for modifying operations when the storage was opened
// in readonly mode.
var ErrReadOnly = logicerr.New("opened as read-only")

// ErrNoSpace MUST be returned when there is no space to put an object on the device.
var ErrNoSpace = logicerr.New("no free space")
