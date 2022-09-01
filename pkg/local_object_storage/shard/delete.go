package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr []oid.Address
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct{}

// SetAddresses is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) SetAddresses(addr ...oid.Address) {
	p.addr = append(p.addr, addr...)
}

// Delete removes data from the shard's writeCache, metaBase and
// blobStor.
func (s *Shard) Delete(prm DeletePrm) (DeleteRes, error) {
	m := s.GetMode()
	if m.ReadOnly() {
		return DeleteRes{}, ErrReadOnlyMode
	} else if m.NoMetabase() {
		return DeleteRes{}, ErrDegradedMode
	}

	ln := len(prm.addr)

	smalls := make(map[oid.Address][]byte, ln)

	for i := range prm.addr {
		if s.hasWriteCache() {
			err := s.writeCache.Delete(prm.addr[i])
			if err != nil && !IsErrNotFound(err) {
				s.log.Error("can't delete object from write cache", zap.String("error", err.Error()))
			}
		}

		var sPrm meta.StorageIDPrm
		sPrm.SetAddress(prm.addr[i])

		res, err := s.metaBase.StorageID(sPrm)
		if err != nil {
			s.log.Debug("can't get blobovniczaID from metabase",
				zap.Stringer("object", prm.addr[i]),
				zap.String("error", err.Error()))

			continue
		}

		if res.StorageID() != nil {
			smalls[prm.addr[i]] = res.StorageID()
		}
	}

	var delPrm meta.DeletePrm
	delPrm.SetAddresses(prm.addr...)

	res, err := s.metaBase.Delete(delPrm)
	if err != nil {
		return DeleteRes{}, err // stop on metabase error ?
	}

	s.decObjectCounterBy(res.RemovedObjects())

	for i := range prm.addr { // delete small object
		var delPrm common.DeletePrm
		delPrm.Address = prm.addr[i]
		id := smalls[prm.addr[i]]
		delPrm.StorageID = id

		_, err = s.blobStor.Delete(delPrm)
		if err != nil {
			s.log.Debug("can't remove small object from blobStor",
				zap.Stringer("object_address", prm.addr[i]),
				zap.String("error", err.Error()))
		}
	}

	return DeleteRes{}, nil
}
