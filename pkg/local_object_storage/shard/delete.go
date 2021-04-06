package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr []*objectSDK.Address
}

// DeleteRes groups resulting values of Delete operation.
type DeleteRes struct{}

// WithAddresses is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) WithAddresses(addr ...*objectSDK.Address) *DeletePrm {
	if p != nil {
		p.addr = append(p.addr, addr...)
	}

	return p
}

// Delete removes data from the shard's writeCache, metaBase and
// blobStor.
func (s *Shard) Delete(prm *DeletePrm) (*DeleteRes, error) {
	ln := len(prm.addr)
	delSmallPrm := new(blobstor.DeleteSmallPrm)
	delBigPrm := new(blobstor.DeleteBigPrm)

	smalls := make(map[*objectSDK.Address]*blobovnicza.ID, ln)

	for i := range prm.addr {
		if s.hasWriteCache() {
			err := s.writeCache.Delete(prm.addr[i])
			if err != nil && !errors.Is(err, object.ErrNotFound) {
				s.log.Error("can't delete object from write cache", zap.String("error", err.Error()))
			}
		}

		blobovniczaID, err := meta.IsSmall(s.metaBase, prm.addr[i])
		if err != nil {
			s.log.Debug("can't get blobovniczaID from metabase",
				zap.Stringer("object", prm.addr[i]),
				zap.String("error", err.Error()))

			continue
		}

		if blobovniczaID != nil {
			smalls[prm.addr[i]] = blobovniczaID
		}
	}

	err := meta.Delete(s.metaBase, prm.addr...)
	if err != nil {
		return nil, err // stop on metabase error ?
	}

	for i := range prm.addr { // delete small object
		if id, ok := smalls[prm.addr[i]]; ok {
			delSmallPrm.SetAddress(prm.addr[i])
			delSmallPrm.SetBlobovniczaID(id)

			_, err = s.blobStor.DeleteSmall(delSmallPrm)
			if err != nil {
				s.log.Debug("can't remove small object from blobStor",
					zap.Stringer("object_address", prm.addr[i]),
					zap.String("error", err.Error()))
			}

			continue
		}

		// delete big object

		delBigPrm.SetAddress(prm.addr[i])

		_, err = s.blobStor.DeleteBig(delBigPrm)
		if err != nil {
			s.log.Debug("can't remove big object from blobStor",
				zap.Stringer("object_address", prm.addr[i]),
				zap.String("error", err.Error()))
		}
	}

	return nil, nil
}
