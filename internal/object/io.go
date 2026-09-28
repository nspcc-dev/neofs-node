package object

import (
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// WriterTo provides object [io.WriterTo].
type WriterTo object.Object

// WriteTo implements [io.WriterTo].
func (x WriterTo) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(object.Object(x).Marshal())
	return int64(n), err
}
