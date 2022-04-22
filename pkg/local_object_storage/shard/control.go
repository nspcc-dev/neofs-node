package shard

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Open opens all Shard's components.
func (s *Shard) Open() error {
	components := []interface{ Open() error }{
		s.blobStor, s.metaBase, s.pilorama,
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
		s.blobStor.Init, fMetabase, s.pilorama.Init,
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
					s.collectExpiredLocks,
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

	obj := objectSDK.New()

	return blobstor.IterateBinaryObjects(s.blobStor, func(addr oid.Address, data []byte, blzID *blobovnicza.ID) error {
		if err := obj.Unmarshal(data); err != nil {
			s.log.Warn("could not unmarshal object",
				zap.Stringer("address", addr),
				zap.String("err", err.Error()))
			return nil
		}

		//nolint: exhaustive
		switch obj.Type() {
		case objectSDK.TypeTombstone:
			tombstone := objectSDK.NewTombstone()

			if err := tombstone.Unmarshal(obj.Payload()); err != nil {
				return fmt.Errorf("could not unmarshal tombstone content: %w", err)
			}

			tombAddr := object.AddressOf(obj)
			memberIDs := tombstone.Members()
			tombMembers := make([]oid.Address, 0, len(memberIDs))

			for i := range memberIDs {
				a := tombAddr
				a.SetObject(memberIDs[i])

				tombMembers = append(tombMembers, a)
			}

			var inhumePrm meta.InhumePrm

			inhumePrm.WithTombstoneAddress(tombAddr)
			inhumePrm.WithAddresses(tombMembers...)

			_, err = s.metaBase.Inhume(inhumePrm)
			if err != nil {
				return fmt.Errorf("could not inhume objects: %w", err)
			}
		case objectSDK.TypeLock:
			var lock objectSDK.Lock
			if err := lock.Unmarshal(obj.Payload()); err != nil {
				return fmt.Errorf("could not unmarshal lock content: %w", err)
			}

			locked := make([]oid.ID, lock.NumberOfMembers())
			lock.ReadMembers(locked)

			cnr, _ := obj.ContainerID()
			id, _ := obj.ID()
			err = s.metaBase.Lock(cnr, id, locked)
			if err != nil {
				return fmt.Errorf("could not lock objects: %w", err)
			}
		}

		err := meta.Put(s.metaBase, obj, blzID)
		if err != nil && !meta.IsErrRemoved(err) {
			return err
		}

		return nil
	})
}

// Close releases all Shard's components.
func (s *Shard) Close() error {
	components := []interface{ Close() error }{}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	components = append(components, s.pilorama, s.blobStor, s.metaBase)

	for _, component := range components {
		if err := component.Close(); err != nil {
			return fmt.Errorf("could not close %s: %w", component, err)
		}
	}

	s.gc.stop()

	return nil
}
