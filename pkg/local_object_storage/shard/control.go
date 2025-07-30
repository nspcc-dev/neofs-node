package shard

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

	for i, component := range components {
		if err := component.Open(false); err != nil {
			if component == s.metaBase {
				// We must first open all other components to avoid
				// opening non-existent DB in read-only mode.
				for j := i + 1; j < len(components); j++ {
					if err := components[j].Open(false); err != nil {
						// Other components must be opened, fail.
						return fmt.Errorf("could not open %T: %w", components[j], err)
					}
				}
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
	return (*Shard)(x).resyncMetabase()
}

// Init initializes all Shard's components.
func (s *Shard) Init() error {
	type initializer interface {
		Init() error
	}

	var components = []initializer{&s.compression, s.blobStor}

	if !s.GetMode().NoMetabase() {
		var initMetabase initializer

		if s.needResyncMetabase() {
			initMetabase = (*metabaseSynchronizer)(s)
		} else {
			initMetabase = s.metaBase
		}

		components = append(components, initMetabase)
	}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	for _, component := range components {
		if err := component.Init(); err != nil {
			if component == s.metaBase {
				if errors.Is(err, meta.ErrOutdatedVersion) {
					return fmt.Errorf("metabase initialization: %w", err)
				}

				err = s.handleMetabaseFailure("init", err)
				if err != nil {
					return err
				}

				break
			}

			return fmt.Errorf("could not initialize %T: %w", component, err)
		}
		if component == s.blobStor {
			s.initedStorage = true
		}
	}

	s.initMetrics()

	s.gc = &gc{
		gcCfg:       &s.gcCfg,
		remover:     s.removeGarbage,
		stopChannel: make(chan struct{}),
		eventChan:   make(chan Event),
		mEventHandler: map[eventType]*eventHandlers{
			eventNewEpoch: {
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

func (s *Shard) resyncMetabase() error {
	err := s.metaBase.Reset()
	if err != nil {
		return fmt.Errorf("could not reset metabase: %w", err)
	}

	if s.writeCache != nil {
		// ensure there will not be any raсes in write-cache -> blobstor object
		// background flushing while iterating blobstor
		err = s.writeCache.Flush(true)
		if err != nil {
			s.log.Warn("could not flush write-cache while resyncing metabase", zap.Error(err))
		}
	}

	var errorHandler = func(addr oid.Address, err error) error {
		s.log.Warn("error occurred during the iteration",
			zap.Stringer("address", addr),
			zap.String("err", err.Error()))
		return nil
	}

	err = s.blobStor.Iterate(s.resyncObjectHandler, errorHandler)
	if err != nil {
		return fmt.Errorf("could not put objects to the meta from blobstor: %w", err)
	}

	err = s.metaBase.SyncCounters()
	if err != nil {
		return fmt.Errorf("could not sync object counters: %w", err)
	}

	return nil
}

func (s *Shard) resyncObjectHandler(addr oid.Address, data []byte) error {
	obj := objectSDK.New()

	if err := obj.Unmarshal(data); err != nil {
		s.log.Warn("could not unmarshal object",
			zap.Stringer("address", addr),
			zap.String("err", err.Error()))
		return nil
	}

	if !version.SysObjTargetShouldBeInHeader(obj.Version()) {
		// nolint: exhaustive
		switch obj.Type() {
		case objectSDK.TypeTombstone:
			tombstone := objectSDK.NewTombstone()
			exp, err := object.Expiration(*obj)
			if err != nil && !errors.Is(err, object.ErrNoExpiration) {
				return fmt.Errorf("tombstone's expiration: %w", err)
			}

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

			_, _, err = s.metaBase.Inhume(tombAddr, exp, false, tombMembers...)
			if err != nil {
				if errors.Is(err, apistatus.ErrObjectLocked) {
					// if we are trying to inhume locked object, likely we are doing
					// something wrong and it should not stop resynchronisation
					s.log.Warn("inhuming locked objects",
						zap.Stringer("TS_address", tombAddr),
						zap.Stringers("targets", memberIDs))
					return nil
				}
				return fmt.Errorf("could not inhume [%s] objects: %w", oidsToString(memberIDs), err)
			}
		case objectSDK.TypeLock:
			var lock objectSDK.Lock
			if err := lock.Unmarshal(obj.Payload()); err != nil {
				return fmt.Errorf("could not unmarshal lock content: %w", err)
			}

			locked := make([]oid.ID, lock.NumberOfMembers())
			lock.ReadMembers(locked)

			err := s.metaBase.Lock(obj.GetContainerID(), obj.GetID(), locked)
			if err != nil {
				return fmt.Errorf("could not lock [%s] objects: %w", oidsToString(locked), err)
			}
		}
	}

	err := s.metaBase.PutResync(obj)
	if err != nil && !errors.Is(err, meta.ErrObjectIsExpired) {
		return err
	}

	return nil
}

func oidsToString(ids []oid.ID) string {
	var res string
	for i, id := range ids {
		res += id.String()
		if i != len(ids)-1 {
			res += ", "
		}
	}

	return res
}

// Close releases all Shard's components.
func (s *Shard) Close() error {
	components := []interface{ Close() error }{}

	if s.hasWriteCache() {
		components = append(components, s.writeCache)
	}

	components = append(components, s.blobStor, &s.compression, s.metaBase)

	var lastErr error
	for _, component := range components {
		if err := component.Close(); err != nil {
			lastErr = err
			s.log.Error("could not close shard component", zap.Error(err))
		}
	}

	// If Init/Open was unsuccessful gc can be nil.
	if s.gc != nil {
		s.gc.stop()
	}

	return lastErr
}

// Reload reloads configuration portions that are necessary.
// If a config option is invalid, it logs an error and returns nil.
// If there was a problem with applying new configuration, an error is returned.
func (s *Shard) Reload(opts ...Option) error {
	// Do not use defaultCfg here missing options need not be reloaded.
	var c cfg
	for i := range opts {
		opts[i](&c)
	}

	s.m.Lock()
	defer s.m.Unlock()

	ok, err := s.metaBase.Reload(c.metaOpts...)
	if err != nil {
		if errors.Is(err, meta.ErrDegradedMode) {
			s.log.Error("can't open metabase, move to a degraded mode", zap.Error(err))
			_ = s.setMode(mode.DegradedReadOnly)
		}
		return err
	}
	if ok {
		var err error
		if c.resyncMetabase {
			// Here we refill metabase only if a new instance was opened. This is a feature,
			// we don't want to hang for some time just because we forgot to change
			// config after the node was updated.
			err = s.resyncMetabase()
		} else {
			err = s.metaBase.Init()
		}
		if err != nil {
			s.log.Error("can't initialize metabase, move to a degraded-read-only mode", zap.Error(err))
			_ = s.setMode(mode.DegradedReadOnly)
			return err
		}
	}

	s.log.Info("trying to set mode", zap.Stringer("mode", c.info.Mode))
	return s.setMode(c.info.Mode)
}
