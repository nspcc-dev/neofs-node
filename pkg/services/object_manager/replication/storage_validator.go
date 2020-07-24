package replication

import (
	"context"
	"time"

	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type (
	// StorageValidator is an interface of entity
	// that listens and performs task of storage validation on remote nodes.
	// Validation can result to the need to replicate or clean object.
	StorageValidator interface {
		Process(ctx context.Context) chan<- *ObjectLocationRecord
		SubscribeReplication(ch chan<- *ReplicateTask)
		SubscribeGarbage(ch chan<- Address)
	}

	storageValidator struct {
		objectVerifier  ObjectVerifier
		log             *zap.Logger
		presenceChecker PresenceChecker
		addrstore       AddressStore

		taskChanCap         int
		resultTimeout       time.Duration
		replicateResultChan chan<- *ReplicateTask
		garbageChan         chan<- Address
	}

	// StorageValidatorParams groups the parameters of storage validator's constructor.
	StorageValidatorParams struct {
		ObjectVerifier
		PresenceChecker
		*zap.Logger

		TaskChanCap   int
		ResultTimeout time.Duration
		AddrStore     AddressStore
	}
)

const (
	defaultStorageValidatorChanCap       = 10
	defaultStorageValidatorResultTimeout = time.Second

	storageValidatorEntity = "storage validator"
)

func (s *storageValidator) SubscribeReplication(ch chan<- *ReplicateTask) {
	s.replicateResultChan = ch
}

func (s *storageValidator) SubscribeGarbage(ch chan<- Address) { s.garbageChan = ch }

func (s *storageValidator) Process(ctx context.Context) chan<- *ObjectLocationRecord {
	ch := make(chan *ObjectLocationRecord, s.taskChanCap)
	go s.processRoutine(ctx, ch)

	return ch
}

func (s *storageValidator) writeReplicateResult(replicateTask *ReplicateTask) {
	if s.replicateResultChan == nil {
		return
	}
	select {
	case s.replicateResultChan <- replicateTask:
	case <-time.After(s.resultTimeout):
		s.log.Warn(writeResultTimeout)
	}
}

func (s *storageValidator) writeGarbage(addr Address) {
	if s.garbageChan == nil {
		return
	}
	select {
	case s.garbageChan <- addr:
	case <-time.After(s.resultTimeout):
		s.log.Warn(writeResultTimeout)
	}
}

func (s *storageValidator) processRoutine(ctx context.Context, taskChan <-chan *ObjectLocationRecord) {
loop:
	for {
		select {
		case <-ctx.Done():
			s.log.Warn(storageValidatorEntity+ctxDoneMsg, zap.Error(ctx.Err()))
			break loop
		case locationRecord, ok := <-taskChan:
			if !ok {
				s.log.Warn(storageValidatorEntity + taskChanClosed)
				break loop
			} else if has, err := s.presenceChecker.Has(locationRecord.Address); err != nil || !has {
				continue loop
			}
			s.handleTask(ctx, locationRecord)
		}
	}
	close(s.replicateResultChan)
	close(s.garbageChan)
}

func (s *storageValidator) handleTask(ctx context.Context, locationRecord *ObjectLocationRecord) {
	selfAddr, err := s.addrstore.SelfAddr()
	if err != nil {
		s.log.Error("storage validator can't obtain self address")
		return
	}

	var (
		weightierCounter int
		replicateTask    = &ReplicateTask{
			Address:      locationRecord.Address,
			Shortage:     locationRecord.ReservationRatio - 1, // taking account of object correctly stored in local store
			ExcludeNodes: nodesFromLocations(locationRecord.Locations, selfAddr),
		}
	)

	for i := range locationRecord.Locations {
		loc := locationRecord.Locations[i]

		if s.objectVerifier.Verify(ctx, &ObjectVerificationParams{
			Address: locationRecord.Address,
			Node:    locationRecord.Locations[i].Node,
			Handler: func(valid bool, _ *Object) {
				if valid {
					replicateTask.Shortage--
					if loc.WeightGreater {
						weightierCounter++
					}
				}
			},
		}); weightierCounter >= locationRecord.ReservationRatio {
			s.writeGarbage(locationRecord.Address)
			return
		}
	}

	if replicateTask.Shortage > 0 {
		s.writeReplicateResult(replicateTask)
	}
}

// nodesFromLocations must ignore self address, because it is used in
// storage validator during replication. We must ignore our own stored
// objects during replication and work with remote hosts and check their
// verification info.
func nodesFromLocations(locations []ObjectLocation, selfaddr multiaddr.Multiaddr) []multiaddr.Multiaddr {
	res := make([]multiaddr.Multiaddr, 0, len(locations))

	for i := range locations {
		if !locations[i].Node.Equal(selfaddr) {
			res = append(res, locations[i].Node)
		}
	}

	return res
}

// NewStorageValidator is a storage validator's constructor.
func NewStorageValidator(p StorageValidatorParams) (StorageValidator, error) {
	switch {
	case p.Logger == nil:
		return nil, instanceError(storageValidatorEntity, loggerPart)
	case p.ObjectVerifier == nil:
		return nil, instanceError(storageValidatorEntity, objectVerifierPart)
	case p.PresenceChecker == nil:
		return nil, instanceError(storageValidatorEntity, presenceCheckerPart)
	case p.AddrStore == nil:
		return nil, instanceError(storageValidatorEntity, addrStorePart)
	}

	if p.TaskChanCap <= 0 {
		p.TaskChanCap = defaultStorageValidatorChanCap
	}

	if p.ResultTimeout <= 0 {
		p.ResultTimeout = defaultStorageValidatorResultTimeout
	}

	return &storageValidator{
		objectVerifier:  p.ObjectVerifier,
		log:             p.Logger,
		presenceChecker: p.PresenceChecker,
		taskChanCap:     p.TaskChanCap,
		resultTimeout:   p.ResultTimeout,
		addrstore:       p.AddrStore,
	}, nil
}
