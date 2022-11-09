package blobovniczatree

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

func isErrOutOfRange(err error) bool {
	return errors.As(err, new(apistatus.ObjectOutOfRange))
}

func isLogical(err error) bool {
	return errors.As(err, new(logicerr.Logical))
}
