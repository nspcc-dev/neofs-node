package replication

import (
	"context"
	"time"

	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type (
	// ObjectReplicator is an interface of entity
	// that listens object replication tasks.
	// Result includes new object storage list.
	ObjectReplicator interface {
		Process(ctx context.Context) chan<- *ReplicateTask
		Subscribe(ch chan<- *ReplicateResult)
	}

	objectReplicator struct {
		objectReceptacle      ObjectReceptacle
		remoteStorageSelector RemoteStorageSelector
		objectSource          ObjectSource
		presenceChecker       PresenceChecker
		log                   *zap.Logger

		taskChanCap   int
		resultTimeout time.Duration
		resultChan    chan<- *ReplicateResult
	}

	// ObjectReplicatorParams groups the parameters of replicator's constructor.
	ObjectReplicatorParams struct {
		RemoteStorageSelector
		ObjectSource
		ObjectReceptacle
		PresenceChecker
		*zap.Logger

		TaskChanCap   int
		ResultTimeout time.Duration
	}
)

const (
	defaultReplicatorChanCap       = 10
	defaultReplicatorResultTimeout = time.Second
	objectReplicatorEntity         = "object replicator"
)

func (s *objectReplicator) Subscribe(ch chan<- *ReplicateResult) { s.resultChan = ch }

func (s *objectReplicator) Process(ctx context.Context) chan<- *ReplicateTask {
	ch := make(chan *ReplicateTask, s.taskChanCap)
	go s.processRoutine(ctx, ch)

	return ch
}

func (s *objectReplicator) writeResult(replicateResult *ReplicateResult) {
	if s.resultChan == nil {
		return
	}
	select {
	case s.resultChan <- replicateResult:
	case <-time.After(s.resultTimeout):
		s.log.Warn(writeResultTimeout)
	}
}

func (s *objectReplicator) processRoutine(ctx context.Context, taskChan <-chan *ReplicateTask) {
loop:
	for {
		select {
		case <-ctx.Done():
			s.log.Warn(objectReplicatorEntity+" process finish: context completed",
				zap.Error(ctx.Err()))
			break loop
		case replicateTask, ok := <-taskChan:
			if !ok {
				s.log.Warn(objectReplicatorEntity + " process finish: task channel closed")
				break loop
			} else if has, err := s.presenceChecker.Has(replicateTask.Address); err != nil || !has {
				continue loop
			}
			s.handleTask(ctx, replicateTask)
		}
	}
	close(s.resultChan)
}

func (s *objectReplicator) handleTask(ctx context.Context, task *ReplicateTask) {
	obj, err := s.objectSource.Get(ctx, task.Address)
	if err != nil {
		s.log.Warn("get object from storage failure", zap.Error(err))
		return
	}

	res := &ReplicateResult{
		ReplicateTask: task,
		NewStorages:   make([]multiaddr.Multiaddr, 0, task.Shortage),
	}

	for len(res.NewStorages) < task.Shortage {
		nodesInfo, err := s.remoteStorageSelector.SelectRemoteStorages(ctx, task.Address, task.ExcludeNodes...)
		if err != nil {
			break
		}

		for i := 0; i < len(nodesInfo); i++ {
			if contains(res.NewStorages, nodesInfo[i].Node) {
				nodesInfo = append(nodesInfo[:i], nodesInfo[i+1:]...)
				i--

				continue
			}
		}

		if len(nodesInfo) > task.Shortage {
			nodesInfo = nodesInfo[:task.Shortage]
		}

		if len(nodesInfo) == 0 {
			break
		}

		if err := s.objectReceptacle.Put(ctx, ObjectStoreParams{
			Object: obj,
			Nodes:  nodesInfo,
			Handler: func(location ObjectLocation, success bool) {
				if success {
					res.NewStorages = append(res.NewStorages, location.Node)
				} else {
					task.ExcludeNodes = append(task.ExcludeNodes, location.Node)
				}
			},
		}); err != nil {
			s.log.Warn("replicate object failure", zap.Error(err))
			break
		}
	}

	s.writeResult(res)
}

func contains(list []multiaddr.Multiaddr, item multiaddr.Multiaddr) bool {
	for i := range list {
		if list[i].Equal(item) {
			return true
		}
	}

	return false
}

// NewReplicator is an object replicator's constructor.
func NewReplicator(p ObjectReplicatorParams) (ObjectReplicator, error) {
	switch {
	case p.ObjectReceptacle == nil:
		return nil, instanceError(objectReplicatorEntity, objectReceptaclePart)
	case p.ObjectSource == nil:
		return nil, instanceError(objectReplicatorEntity, objectSourcePart)
	case p.RemoteStorageSelector == nil:
		return nil, instanceError(objectReplicatorEntity, remoteStorageSelectorPart)
	case p.PresenceChecker == nil:
		return nil, instanceError(objectReplicatorEntity, presenceCheckerPart)
	case p.Logger == nil:
		return nil, instanceError(objectReplicatorEntity, loggerPart)
	}

	if p.TaskChanCap <= 0 {
		p.TaskChanCap = defaultReplicatorChanCap
	}

	if p.ResultTimeout <= 0 {
		p.ResultTimeout = defaultReplicatorResultTimeout
	}

	return &objectReplicator{
		objectReceptacle:      p.ObjectReceptacle,
		remoteStorageSelector: p.RemoteStorageSelector,
		objectSource:          p.ObjectSource,
		presenceChecker:       p.PresenceChecker,
		log:                   p.Logger,
		taskChanCap:           p.TaskChanCap,
		resultTimeout:         p.ResultTimeout,
	}, nil
}
