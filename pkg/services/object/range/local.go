package rangesvc

import (
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/pkg/errors"
)

type localRangeWriter struct {
	addr *object.Address

	rng *object.Range

	storage *localstore.Storage
}

func (l *localRangeWriter) WriteTo(w io.Writer) (int64, error) {
	obj, err := l.storage.Get(l.addr)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get object from local storage", l)
	}

	payload := obj.Payload()
	left := l.rng.GetOffset()
	right := left + l.rng.GetLength()

	if ln := uint64(len(payload)); ln < right {
		return 0, errors.Errorf("(%T) object range is out-of-boundaries (size %d, range [%d:%d]", l, ln, left, right)
	}

	n, err := w.Write(payload[left:right])

	return int64(n), err
}
