package blobovniczatree

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

func isErrOutOfRange(err error) bool {
	return errors.As(err, new(apistatus.ObjectOutOfRange))
}
