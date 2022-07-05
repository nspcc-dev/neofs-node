package blobovniczatree

import (
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (b *Blobovniczas) Exists(addr oid.Address) (bool, error) {
	activeCache := make(map[string]struct{})

	var prm blobovnicza.GetPrm
	prm.SetAddress(addr)

	var found bool
	err := b.iterateSortedLeaves(&addr, func(p string) (bool, error) {
		dirPath := filepath.Dir(p)

		_, ok := activeCache[dirPath]

		_, err := b.getObjectFromLevel(prm, p, !ok)
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

	return found, err
}
