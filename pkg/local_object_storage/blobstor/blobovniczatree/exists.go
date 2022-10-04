package blobovniczatree

import (
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// Exists implements common.Storage.
func (b *Blobovniczas) Exists(prm common.ExistsPrm) (common.ExistsRes, error) {
	activeCache := make(map[string]struct{})

	var gPrm blobovnicza.GetPrm
	gPrm.SetAddress(prm.Address)

	var found bool
	err := b.iterateSortedLeaves(&prm.Address, func(p string) (bool, error) {
		dirPath := filepath.Dir(p)

		_, ok := activeCache[dirPath]

		_, err := b.getObjectFromLevel(gPrm, p, !ok)
		if err != nil {
			if !blobovnicza.IsErrNotFound(err) {
				b.log.Debug("could not get object from level",
					logger.FieldString("level", p),
					logger.FieldError(err))
			}
		}

		activeCache[dirPath] = struct{}{}
		found = err == nil
		return found, nil
	})

	return common.ExistsRes{Exists: found}, err
}
