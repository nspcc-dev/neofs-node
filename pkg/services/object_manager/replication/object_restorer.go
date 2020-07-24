package replication

import (
	"context"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"go.uber.org/zap"
)

type (
	// ObjectRestorer is an interface of entity
	// that listen tasks to restore object by address.
	// Restorer doesn't recheck if object is actually corrupted.
	// Restorer writes result to subscriber only if restoration was successful.
	ObjectRestorer interface {
		Process(ctx context.Context) chan<- Address
		Subscribe(ch chan<- Address)
	}

	objectRestorer struct {
		objectVerifier        ObjectVerifier
		remoteStorageSelector RemoteStorageSelector
		objectReceptacle      ObjectReceptacle
		epochReceiver         EpochReceiver
		presenceChecker       PresenceChecker
		log                   *zap.Logger

		taskChanCap   int
		resultTimeout time.Duration
		resultChan    chan<- Address
	}

	// ObjectRestorerParams groups the parameters of object restorer's constructor.
	ObjectRestorerParams struct {
		ObjectVerifier
		ObjectReceptacle
		EpochReceiver
		RemoteStorageSelector
		PresenceChecker
		*zap.Logger

		TaskChanCap   int
		ResultTimeout time.Duration
	}
)

const (
	defaultRestorerChanCap       = 10
	defaultRestorerResultTimeout = time.Second
	objectRestorerEntity         = "object restorer"
)

func (s *objectRestorer) Subscribe(ch chan<- Address) { s.resultChan = ch }

func (s *objectRestorer) Process(ctx context.Context) chan<- Address {
	ch := make(chan Address, s.taskChanCap)
	go s.processRoutine(ctx, ch)

	return ch
}

func (s *objectRestorer) writeResult(refInfo Address) {
	if s.resultChan == nil {
		return
	}
	select {
	case s.resultChan <- refInfo:
	case <-time.After(s.resultTimeout):
		s.log.Warn(writeResultTimeout)
	}
}

func (s *objectRestorer) processRoutine(ctx context.Context, taskChan <-chan Address) {
loop:
	for {
		select {
		case <-ctx.Done():
			s.log.Warn(objectRestorerEntity+ctxDoneMsg, zap.Error(ctx.Err()))
			break loop
		case addr, ok := <-taskChan:
			if !ok {
				s.log.Warn(objectRestorerEntity + taskChanClosed)
				break loop
			} else if has, err := s.presenceChecker.Has(addr); err != nil || !has {
				continue loop
			}
			s.handleTask(ctx, addr)
		}
	}
	close(s.resultChan)
}

func (s *objectRestorer) handleTask(ctx context.Context, addr Address) {
	var (
		receivedObj *Object
		exclNodes   = make([]multiaddr.Multiaddr, 0)
	)

loop:
	for {
		nodesInfo, err := s.remoteStorageSelector.SelectRemoteStorages(ctx, addr, exclNodes...)
		if err != nil {
			break
		}

		for i := range nodesInfo {
			info := nodesInfo[i]
			if s.objectVerifier.Verify(ctx, &ObjectVerificationParams{
				Address: addr,
				Node:    nodesInfo[i].Node,
				Handler: func(valid bool, obj *Object) {
					if valid {
						receivedObj = obj
					} else {
						exclNodes = append(exclNodes, info.Node)
					}
				},
				LocalInvalid: true,
			}) {
				break loop
			}
		}
	}

	if err := s.objectReceptacle.Put(
		context.WithValue(ctx, localstore.StoreEpochValue, s.epochReceiver.Epoch()),
		ObjectStoreParams{Object: receivedObj},
	); err != nil {
		s.log.Warn("put object to local storage failure", append(addressFields(addr), zap.Error(err))...)
		return
	}

	s.writeResult(addr)
}

// NewObjectRestorer is an object restorer's constructor.
func NewObjectRestorer(p *ObjectRestorerParams) (ObjectRestorer, error) {
	switch {
	case p.Logger == nil:
		return nil, instanceError(objectRestorerEntity, loggerPart)
	case p.ObjectVerifier == nil:
		return nil, instanceError(objectRestorerEntity, objectVerifierPart)
	case p.ObjectReceptacle == nil:
		return nil, instanceError(objectRestorerEntity, objectReceptaclePart)
	case p.RemoteStorageSelector == nil:
		return nil, instanceError(objectRestorerEntity, remoteStorageSelectorPart)
	case p.EpochReceiver == nil:
		return nil, instanceError(objectRestorerEntity, epochReceiverPart)
	case p.PresenceChecker == nil:
		return nil, instanceError(objectRestorerEntity, presenceCheckerPart)
	}

	if p.TaskChanCap <= 0 {
		p.TaskChanCap = defaultRestorerChanCap
	}

	if p.ResultTimeout <= 0 {
		p.ResultTimeout = defaultRestorerResultTimeout
	}

	return &objectRestorer{
		objectVerifier:        p.ObjectVerifier,
		remoteStorageSelector: p.RemoteStorageSelector,
		objectReceptacle:      p.ObjectReceptacle,
		epochReceiver:         p.EpochReceiver,
		presenceChecker:       p.PresenceChecker,
		log:                   p.Logger,
		taskChanCap:           p.TaskChanCap,
		resultTimeout:         p.ResultTimeout,
	}, nil
}
