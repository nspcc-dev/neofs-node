package headsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/pkg/errors"
)

type localHeader struct {
	storage *localstore.Storage
}

func (h *localHeader) head(ctx context.Context, prm *Prm, handler func(*object.Object)) error {
	head, err := h.storage.Head(prm.addr)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get header from local storage", h)
	}

	handler(head)

	return nil
}
