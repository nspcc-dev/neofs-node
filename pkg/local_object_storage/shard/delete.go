package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
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
	var delSmallPrm blobovniczatree.DeleteSmallPrm
	var delBigPrm blobstor.DeleteBigPrm

	smalls := make(map[oid.Address]*blobovnicza.ID, ln)

	for i := range prm.addr {
		if s.hasWriteCache() {
			err := s.writeCache.Delete(prm.addr[i])
			if err != nil && !writecache.IsErrNotFound(err) {
				s.log.Error("can't delete object from write cache", zap.String("error", err.Error()))
			}
		}

		var sPrm meta.IsSmallPrm
		sPrm.WithAddress(prm.addr[i])

		res, err := s.metaBase.IsSmall(sPrm)
		if err != nil {
			s.log.Debug("can't get blobovniczaID from metabase",
				zap.Stringer("object", prm.addr[i]),
				zap.String("error", err.Error()))

			continue
		}

		if res.BlobovniczaID() != nil {
			smalls[prm.addr[i]] = res.BlobovniczaID()
		}
	}

	var delPrm meta.DeletePrm
	delPrm.SetAddresses(prm.addr...)

	_, err := s.metaBase.Delete(delPrm)
	if err != nil {
		return DeleteRes{}, err // stop on metabase error ?
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

	return DeleteRes{}, nil
}
