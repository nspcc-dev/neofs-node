package replication

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type (
	// Manager is an interface of object manager,
	Manager interface {
		Process(ctx context.Context)
		HandleEpoch(ctx context.Context, epoch uint64)
	}

	manager struct {
		objectPool     ObjectPool
		managerTimeout time.Duration
		objectVerifier ObjectVerifier
		log            *zap.Logger

		locationDetector ObjectLocationDetector
		storageValidator StorageValidator
		replicator       ObjectReplicator
		restorer         ObjectRestorer
		placementHonorer PlacementHonorer

		// internal task channels
		detectLocationTaskChan chan<- Address
		restoreTaskChan        chan<- Address

		pushTaskTimeout time.Duration

		// internal result channels
		replicationResultChan <-chan *ReplicateResult
		restoreResultChan     <-chan Address

		garbageChanCap         int
		replicateResultChanCap int
		restoreResultChanCap   int

		garbageChan  <-chan Address
		garbageStore *garbageStore

		epochCh   chan uint64
		scheduler Scheduler

		poolSize          int
		poolExpansionRate float64
	}

	// ManagerParams groups the parameters of object manager's constructor.
	ManagerParams struct {
		Interval                time.Duration
		PushTaskTimeout         time.Duration
		PlacementHonorerEnabled bool
		ReplicateTaskChanCap    int
		RestoreTaskChanCap      int
		GarbageChanCap          int
		InitPoolSize            int
		ExpansionRate           float64

		ObjectPool
		ObjectVerifier

		PlacementHonorer
		ObjectLocationDetector
		StorageValidator
		ObjectReplicator
		ObjectRestorer

		*zap.Logger

		Scheduler
	}
)

const (
	managerEntity = "replication manager"

	redundantCopiesBeagleName = "BEAGLE_REDUNDANT_COPIES"

	defaultInterval        = 3 * time.Second
	defaultPushTaskTimeout = time.Second

	defaultGarbageChanCap         = 10
	defaultReplicateResultChanCap = 10
	defaultRestoreResultChanCap   = 10
)

func (s *manager) Name() string { return redundantCopiesBeagleName }

func (s *manager) HandleEpoch(ctx context.Context, epoch uint64) {
	select {
	case s.epochCh <- epoch:
	case <-ctx.Done():
		return
	case <-time.After(s.managerTimeout):
		// this timeout must never happen
		// if timeout happens in runtime, then something is definitely wrong!
		s.log.Warn("replication scheduler is busy")
	}
}

func (s *manager) Process(ctx context.Context) {
	// starting object restorer
	// bind manager to push restore tasks to restorer
	s.restoreTaskChan = s.restorer.Process(ctx)

	// bind manager to listen object restorer results
	restoreResultChan := make(chan Address, s.restoreResultChanCap)
	s.restoreResultChan = restoreResultChan
	s.restorer.Subscribe(restoreResultChan)

	// starting location detector
	// bind manager to push locate tasks to location detector
	s.detectLocationTaskChan = s.locationDetector.Process(ctx)

	locationsHandlerStartFn := s.storageValidator.Process
	if s.placementHonorer != nil {
		locationsHandlerStartFn = s.placementHonorer.Process

		// starting storage validator
		// bind placement honorer to push validate tasks to storage validator
		s.placementHonorer.Subscribe(s.storageValidator.Process(ctx))
	}

	// starting location handler component
	// bind location detector to push tasks to location handler component
	s.locationDetector.Subscribe(locationsHandlerStartFn(ctx))

	// bind manager to listen object replicator results
	replicateResultChan := make(chan *ReplicateResult, s.replicateResultChanCap)
	s.replicationResultChan = replicateResultChan
	s.replicator.Subscribe(replicateResultChan)

	// starting replicator
	// bind storage validator to push replicate tasks to replicator
	s.storageValidator.SubscribeReplication(s.replicator.Process(ctx))
	garbageChan := make(chan Address, s.garbageChanCap)
	s.garbageChan = garbageChan
	s.storageValidator.SubscribeGarbage(garbageChan)

	go s.taskRoutine(ctx)
	go s.resultRoutine(ctx)
	s.processRoutine(ctx)
}

func resultLog(s1, s2 string) string {
	return fmt.Sprintf(managerEntity+" %s process finish: %s", s1, s2)
}

func (s *manager) writeDetectLocationTask(addr Address) {
	if s.detectLocationTaskChan == nil {
		return
	}
	select {
	case s.detectLocationTaskChan <- addr:
	case <-time.After(s.pushTaskTimeout):
		s.log.Warn(writeResultTimeout)
	}
}

func (s *manager) writeRestoreTask(addr Address) {
	if s.restoreTaskChan == nil {
		return
	}
	select {
	case s.restoreTaskChan <- addr:
	case <-time.After(s.pushTaskTimeout):
		s.log.Warn(writeResultTimeout)
	}
}

func (s *manager) resultRoutine(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			s.log.Warn(resultLog("result", ctxDoneMsg), zap.Error(ctx.Err()))
			break loop
		case addr, ok := <-s.restoreResultChan:
			if !ok {
				s.log.Warn(resultLog("result", "restorer result channel closed"))
				break loop
			}
			s.log.Info("object successfully restored", addressFields(addr)...)
		case res, ok := <-s.replicationResultChan:
			if !ok {
				s.log.Warn(resultLog("result", "replicator result channel closed"))
				break loop
			} else if len(res.NewStorages) > 0 {
				s.log.Info("object successfully replicated",
					append(addressFields(res.Address), zap.Any("new storages", res.NewStorages))...)
			}
		case addr, ok := <-s.garbageChan:
			if !ok {
				s.log.Warn(resultLog("result", "garbage channel closed"))
				break loop
			}
			s.garbageStore.put(addr)
		}
	}
}

func (s *manager) taskRoutine(ctx context.Context) {
loop:
	for {
		if task, err := s.objectPool.Pop(); err == nil {
			select {
			case <-ctx.Done():
				s.log.Warn(resultLog("task", ctxDoneMsg), zap.Error(ctx.Err()))
				break loop
			default:
				s.distributeTask(ctx, task)
			}
		} else {
			// if object pool is empty, check it again after a while
			time.Sleep(s.managerTimeout)
		}
	}
	close(s.restoreTaskChan)
	close(s.detectLocationTaskChan)
}

func (s *manager) processRoutine(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case epoch := <-s.epochCh:
			var delta int

			// undone - amount of objects we couldn't process in last epoch
			undone := s.objectPool.Undone()
			if undone > 0 {
				// if there are unprocessed objects, then lower your estimation
				delta = -undone
			} else {
				// otherwise try to expand
				delta = int(float64(s.poolSize) * s.poolExpansionRate)
			}

			tasks, err := s.scheduler.SelectForReplication(s.poolSize + delta)
			if err != nil {
				s.log.Warn("can't select objects for replication", zap.Error(err))
			}

			// if there are NOT enough objects to fill the pool, do not change it
			// otherwise expand or shrink it with the delta value
			if len(tasks) >= s.poolSize+delta {
				s.poolSize += delta
			}

			s.objectPool.Update(tasks)

			s.log.Info("replication schedule updated",
				zap.Int("unprocessed_tasks", undone),
				zap.Int("next_tasks", len(tasks)),
				zap.Int("pool_size", s.poolSize),
				zap.Uint64("new_epoch", epoch))
		}
	}
}

// Function takes object from storage by address (if verify
// If verify flag is set object stored incorrectly (Verify returned error) - restore task is planned
// otherwise validate task is planned.
func (s *manager) distributeTask(ctx context.Context, addr Address) {
	if !s.objectVerifier.Verify(ctx, &ObjectVerificationParams{Address: addr}) {
		s.writeRestoreTask(addr)
		return
	}

	s.writeDetectLocationTask(addr)
}

// NewManager is an object manager's constructor.
func NewManager(p ManagerParams) (Manager, error) {
	switch {
	case p.ObjectPool == nil:
		return nil, instanceError(managerEntity, objectPoolPart)
	case p.ObjectVerifier == nil:
		return nil, instanceError(managerEntity, objectVerifierPart)
	case p.Logger == nil:
		return nil, instanceError(managerEntity, loggerPart)
	case p.ObjectLocationDetector == nil:
		return nil, instanceError(managerEntity, locationDetectorEntity)
	case p.StorageValidator == nil:
		return nil, instanceError(managerEntity, storageValidatorEntity)
	case p.ObjectReplicator == nil:
		return nil, instanceError(managerEntity, objectReplicatorEntity)
	case p.ObjectRestorer == nil:
		return nil, instanceError(managerEntity, objectRestorerEntity)
	case p.PlacementHonorer == nil && p.PlacementHonorerEnabled:
		return nil, instanceError(managerEntity, placementHonorerEntity)
	case p.Scheduler == nil:
		return nil, instanceError(managerEntity, replicationSchedulerEntity)
	}

	if p.Interval <= 0 {
		p.Interval = defaultInterval
	}

	if p.PushTaskTimeout <= 0 {
		p.PushTaskTimeout = defaultPushTaskTimeout
	}

	if p.GarbageChanCap <= 0 {
		p.GarbageChanCap = defaultGarbageChanCap
	}

	if p.ReplicateTaskChanCap <= 0 {
		p.ReplicateTaskChanCap = defaultReplicateResultChanCap
	}

	if p.RestoreTaskChanCap <= 0 {
		p.RestoreTaskChanCap = defaultRestoreResultChanCap
	}

	if !p.PlacementHonorerEnabled {
		p.PlacementHonorer = nil
	}

	return &manager{
		objectPool:             p.ObjectPool,
		managerTimeout:         p.Interval,
		objectVerifier:         p.ObjectVerifier,
		log:                    p.Logger,
		locationDetector:       p.ObjectLocationDetector,
		storageValidator:       p.StorageValidator,
		replicator:             p.ObjectReplicator,
		restorer:               p.ObjectRestorer,
		placementHonorer:       p.PlacementHonorer,
		pushTaskTimeout:        p.PushTaskTimeout,
		garbageChanCap:         p.GarbageChanCap,
		replicateResultChanCap: p.ReplicateTaskChanCap,
		restoreResultChanCap:   p.RestoreTaskChanCap,
		garbageStore:           newGarbageStore(),
		epochCh:                make(chan uint64),
		scheduler:              p.Scheduler,
		poolSize:               p.InitPoolSize,
		poolExpansionRate:      p.ExpansionRate,
	}, nil
}
