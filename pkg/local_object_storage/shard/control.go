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

func (s *Shard) handleMetabaseFailure(stage string, err error) error {
	s.log.Error("metabase failure, switching mode",
		zap.String("stage", stage),
		zap.Stringer("mode", ModeDegraded),
		zap.Error(err),
	)

	err = s.SetMode(ModeDegraded)
	if err != nil {
		return fmt.Errorf("could not switch to mode %s", ModeDegraded)
	}

	return nil
}

// Open opens all Shard's components.
func (s *Shard) Open() error {
	components := []interface{ Open() error }{
		s.blobStor, s.pilorama,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for _, component := range components {
		if err := component.Open(); err != nil {
			if component == s.metaBase {
				err = s.handleMetabaseFailure("open", err)
				if err != nil {
					return err
				}

				continue
			}

			return fmt.Errorf("could not open %T: %w", component, err)
		}
	}

	return nil
}

type metabaseSynchronizer Shard

func (x *metabaseSynchronizer) Init() error {
	return (*Shard)(x).refillMetabase()
}

// Init initializes all Shard's components.
func (s *Shard) Init() error {
	type initializer interface {
		Init() error
	}

	var components []initializer

	if s.GetMode() != ModeDegraded {
		var initMetabase initializer

		if s.needRefillMetabase() {
			initMetabase = (*metabaseSynchronizer)(s)
		} else {
			initMetabase = s.metaBase
		}

		components = []initializer{
			s.blobStor, initMetabase, s.pilorama,
		}
	} else {
		components = []initializer{s.blobStor, s.pilorama}
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for _, component := range components {
		if err := component.Init(); err != nil {
			if component == s.metaBase {
				err = s.handleMetabaseFailure("init", err)
				if err != nil {
					return err
				}

				continue
			}

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

	return blobstor.IterateObjects(s.blobStor, func(obj *objectSDK.Object, blzID *blobovnicza.ID) error {
		if obj.Type() == objectSDK.TypeTombstone {
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

	components = append(components, s.pilorama, s.blobStor)

	if s.GetMode() != ModeDegraded {
		components = append(components, s.metaBase)
	}

	for _, component := range components {
		if err := component.Close(); err != nil {
			return fmt.Errorf("could not close %s: %w", component, err)
		}
	}

	s.gc.stop()

	return nil
}
