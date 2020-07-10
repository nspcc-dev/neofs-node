package objutil

import (
	"bytes"
	"context"

	"github.com/nspcc-dev/neofs-api-go/object"
)

// Verifier is an interface for checking whether an object conforms to a certain criterion.
// Nil error is equivalent to matching the criterion.
type Verifier interface {
	Verify(context.Context, *object.Object) error
}

// MarshalHeaders marshals all object headers which are "higher" than to-th extended header.
func MarshalHeaders(obj *object.Object, to int) ([]byte, error) {
	buf := new(bytes.Buffer)

	if sysHdr, err := obj.SystemHeader.Marshal(); err != nil {
		return nil, err
	} else if _, err := buf.Write(sysHdr); err != nil {
		return nil, err
	}

	for i := range obj.Headers[:to] {
		if header, err := obj.Headers[i].Marshal(); err != nil {
			return nil, err
		} else if _, err := buf.Write(header); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
