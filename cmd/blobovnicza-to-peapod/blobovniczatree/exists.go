package blobovniczatree

import (
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/cmd/blobovnicza-to-peapod/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"go.uber.org/zap"
)

// Exists implements common.Storage.
func (b *Blobovniczas) Exists(prm common.ExistsPrm) (common.ExistsRes, error) {
	if prm.StorageID != nil {
		id := blobovnicza.NewIDFromBytes(prm.StorageID)
		blz, err := b.openBlobovnicza(id.String())
		if err != nil {
			return common.ExistsRes{}, err
		}

		exists, err := blz.Exists(prm.Address)
		return common.ExistsRes{Exists: exists}, err
	}

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
					zap.String("level", p),
					zap.String("error", err.Error()))
			}
		}

		activeCache[dirPath] = struct{}{}
		found = err == nil
		return found, nil
	})

	return common.ExistsRes{Exists: found}, err
}
