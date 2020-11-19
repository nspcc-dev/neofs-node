package rangesvc

import (
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/pkg/errors"
)

type localRangeWriter struct {
	addr *object.Address

	rng *object.Range

	storage *engine.StorageEngine
}

func (l *localRangeWriter) WriteTo(w io.Writer) (int64, error) {
	rngData, err := engine.GetRange(l.storage, l.addr, l.rng)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get object from local storage", l)
	}

	n, err := w.Write(rngData)

	return int64(n), err
}
