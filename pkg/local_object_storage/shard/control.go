package shard

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (s *Shard) handleMetabaseFailure(stage string, err error) error {
	s.log.Error("metabase failure, switching mode",
		zap.String("stage", stage),
		zap.Stringer("mode", mode.ReadOnly),
		zap.Error(err))

	err = s.SetMode(mode.ReadOnly)
	if err == nil {
		return nil
	}

	s.log.Error("can't move shard to readonly, switch mode",
		zap.String("stage", stage),
		zap.Stringer("mode", mode.DegradedReadOnly),
		zap.Error(err))

	err = s.SetMode(mode.DegradedReadOnly)
	if err != nil {
		return fmt.Errorf("could not switch to mode %s", mode.DegradedReadOnly)
	}
	return nil
}

// Open opens all Shard's components.
func (s *Shard) Open() error {
	components := []interface{ Open(bool) error }{
		s.blobStor, s.metaBase,
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	if s.pilorama != nil {
		components = append(components, s.pilorama)
	}

	for _, component := range components {
		if err := component.Open(false); err != nil {
			if component == s.metaBase {
				err = s.handleMetabaseFailure("open", err)
				if err != nil {
					return err
				}

				break
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

	if !s.GetMode().NoMetabase() {
		var initMetabase initializer

		if s.needRefillMetabase() {
			initMetabase = (*metabaseSynchronizer)(s)
		} else {
			initMetabase = s.metaBase
		}

		components = []initializer{
			s.blobStor, initMetabase,
		}
	} else {
		components = []initializer{s.blobStor}
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	if s.pilorama != nil {
		components = append(components, s.pilorama)
	}

	for _, component := range components {
		if err := component.Init(); err != nil {
			if component == s.metaBase {
				err = s.handleMetabaseFailure("init", err)
				if err != nil {
					return err
				}

				break
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

			inhumePrm.SetTombstoneAddress(tombAddr)
			inhumePrm.SetAddresses(tombMembers...)

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

		var mPrm meta.PutPrm
		mPrm.SetObject(obj)
		mPrm.SetBlobovniczaID(blzID)

		_, err := s.metaBase.Put(mPrm)
		if err != nil && !meta.IsErrRemoved(err) && !errors.Is(err, object.ErrObjectIsExpired) {
			return err
		}

		return nil
	})
}

// Close releases all Shard's components.
func (s *Shard) Close() error {
	components := []interface{ Close() error }{}

	if s.pilorama != nil {
		components = append(components, s.pilorama)
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	components = append(components, s.blobStor, s.metaBase)

	for _, component := range components {
		if err := component.Close(); err != nil {
			return fmt.Errorf("could not close %s: %w", component, err)
		}
	}

	s.gc.stop()

	return nil
}
