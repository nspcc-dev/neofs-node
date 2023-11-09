package shard

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"golang.org/x/sync/errgroup"
)

type ContainerSizePrm struct {
	cnr cid.ID
}

type ContainerSizeRes struct {
	size uint64
}

func (p *ContainerSizePrm) SetContainerID(cnr cid.ID) {
	p.cnr = cnr
}

func (r ContainerSizeRes) Size() uint64 {
	return r.size
}

func (s *Shard) ContainerSize(prm ContainerSizePrm) (ContainerSizeRes, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return ContainerSizeRes{}, ErrDegradedMode
	}

	size, err := s.metaBase.ContainerSize(prm.cnr)
	if err != nil {
		return ContainerSizeRes{}, fmt.Errorf("could not get container size: %w", err)
	}

	return ContainerSizeRes{
		size: size,
	}, nil
}

// DeleteContainer deletes any information related to the container
// including:
// - Metabase;
// - Blobstor;
// - Pilorama (if configured);
// - Write-cache (if configured).
func (s *Shard) DeleteContainer(_ context.Context, cID cid.ID) error {
	s.m.RLock()
	defer s.m.RUnlock()

	var wg errgroup.Group

	objs, err := s.metaBase.ListContainerObjects(cID)
	if err != nil {
		return fmt.Errorf("fetching container objects: %w", err)
	}

	addresses := objectsToAddresses(objs, cID)

	if s.cfg.useWriteCache {
		wg.Go(func() error {
			for _, addr := range addresses {
				err := s.writeCache.Delete(addr)
				if err != nil && !errors.As(err, new(apistatus.ObjectNotFound)) {
					return fmt.Errorf("removing %s obj from write-cache: %w", addr, err)
				}
			}

			return nil
		})
	}

	wg.Go(func() error {
		for _, addr := range addresses {
			_, err := s.blobStor.Delete(common.DeletePrm{Address: addr})
			if err != nil && !errors.As(err, new(apistatus.ObjectNotFound)) {
				return fmt.Errorf("removing %s obj from blobstor: %w", addr, err)
			}
		}

		return nil
	})

	if s.pilorama != nil {
		wg.Go(func() error {
			err := s.TreeDrop(cID, "")
			if err != nil {
				return fmt.Errorf("removing trees for %s container: %w", cID, err)
			}

			return nil
		})
	}

	err = wg.Wait()
	if err != nil {
		return fmt.Errorf("removing objects from storage: %w", err)
	}

	var metaDeletePrm meta.DeletePrm
	metaDeletePrm.SetAddresses(addresses...)

	res, err := s.metaBase.Delete(metaDeletePrm)
	if err != nil {
		return fmt.Errorf("removing object from metabase: %w", err)
	}

	s.decObjectCounterBy(physical, res.RawObjectsRemoved())
	s.decObjectCounterBy(logical, res.AvailableObjectsRemoved())

	var totalRemovedPayload uint64
	for _, s := range res.RemovedObjectSizes() {
		totalRemovedPayload += s
	}

	s.addToPayloadCounter(-int64(totalRemovedPayload))

	err = s.metaBase.DeleteContainer(cID)
	if err != nil {
		return fmt.Errorf("cleaning up '%s' container: %w", cID, err)
	}

	return nil
}

func objectsToAddresses(oo []oid.ID, cID cid.ID) []oid.Address {
	var addr oid.Address
	addr.SetContainer(cID)

	res := make([]oid.Address, 0, len(oo))
	for _, obj := range oo {
		addr.SetObject(obj)
		res = append(res, addr)
	}

	return res
}
