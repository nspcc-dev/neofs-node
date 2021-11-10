package shard

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// Open opens all Shard's components.
func (s *Shard) Open() error {
	components := []interface{ Open() error }{
		s.blobStor, s.metaBase,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for _, component := range components {
		if err := component.Open(); err != nil {
			return fmt.Errorf("could not open %T: %w", component, err)
		}
	}

	return nil
}

// Init initializes all Shard's components.
func (s *Shard) Init() error {
	var fMetabase func() error

	if s.needRefillMetabase() {
		fMetabase = s.refillMetabase
	} else {
		fMetabase = s.metaBase.Init
	}

	components := []func() error{
		s.blobStor.Init, fMetabase,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache.Init)
	}

	for _, component := range components {
		if err := component(); err != nil {
			return fmt.Errorf("could not initialize %T: %w", component, err)
		}
	}

	s.gc = &gc{
		gcCfg:       s.gcCfg,
		remover:     s.removeGarbage,
		stopChannel: make(chan struct{}),
		mEventHandler: map[eventType]*eventHandlers{
			eventNewEpoch: {
				cancelFunc: func() {},
				handlers: []eventHandler{
					s.collectExpiredObjects,
					s.collectExpiredTombstones,
				},
			},
		},
	}

	s.gc.init()

	return nil
}

func (s *Shard) refillMetabase() error {
	err := s.metaBase.Reset()
	if err != nil {
		return fmt.Errorf("could not reset metabase: %w", err)
	}

	return blobstor.IterateObjects(s.blobStor, func(obj *object.Object, blzID *blobovnicza.ID) error {
		if obj.Type() == objectSDK.TypeTombstone {
			tombstone := objectSDK.NewTombstone()

			if err := tombstone.Unmarshal(obj.Payload()); err != nil {
				return fmt.Errorf("could not unmarshal tombstone content: %w", err)
			}

			tombAddr := obj.Address()
			cid := tombAddr.ContainerID()
			memberIDs := tombstone.Members()
			tombMembers := make([]*objectSDK.Address, 0, len(memberIDs))

			for _, id := range memberIDs {
				if id == nil {
					return errors.New("empty member in tombstone")
				}

				a := objectSDK.NewAddress()
				a.SetContainerID(cid)
				a.SetObjectID(id)

				tombMembers = append(tombMembers, a)
			}

			var inhumePrm meta.InhumePrm

			inhumePrm.WithTombstoneAddress(tombAddr)
			inhumePrm.WithAddresses(tombMembers...)

			_, err = s.metaBase.Inhume(&inhumePrm)
			if err != nil {
				return fmt.Errorf("could not inhume objects: %w", err)
			}
		}

		err := meta.Put(s.metaBase, obj, blzID)
		if err != nil && !errors.Is(err, object.ErrAlreadyRemoved) {
			return err
		}

		return nil
	})
}

// Close releases all Shard's components.
func (s *Shard) Close() error {
	components := []interface{ Close() error }{
		s.blobStor, s.metaBase,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for _, component := range components {
		if err := component.Close(); err != nil {
			return fmt.Errorf("could not close %s: %w", component, err)
		}
	}

	s.gc.stop()

	return nil
}
