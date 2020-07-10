package replication

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type (
	// ObjectLocationDetector is an interface of entity
	// that listens tasks to detect object current locations in network.
	ObjectLocationDetector interface {
		Process(ctx context.Context) chan<- Address
		Subscribe(ch chan<- *ObjectLocationRecord)
	}

	objectLocationDetector struct {
		weightComparator         WeightComparator
		objectLocator            ObjectLocator
		reservationRatioReceiver ReservationRatioReceiver
		presenceChecker          PresenceChecker
		log                      *zap.Logger

		taskChanCap   int
		resultTimeout time.Duration
		resultChan    chan<- *ObjectLocationRecord
	}

	// LocationDetectorParams groups the parameters of location detector's constructor.
	LocationDetectorParams struct {
		WeightComparator
		ObjectLocator
		ReservationRatioReceiver
		PresenceChecker
		*zap.Logger

		TaskChanCap   int
		ResultTimeout time.Duration
	}
)

const (
	defaultLocationDetectorChanCap       = 10
	defaultLocationDetectorResultTimeout = time.Second
	locationDetectorEntity               = "object location detector"
)

func (s *objectLocationDetector) Subscribe(ch chan<- *ObjectLocationRecord) { s.resultChan = ch }

func (s *objectLocationDetector) Process(ctx context.Context) chan<- Address {
	ch := make(chan Address, s.taskChanCap)
	go s.processRoutine(ctx, ch)

	return ch
}

func (s *objectLocationDetector) writeResult(locationRecord *ObjectLocationRecord) {
	if s.resultChan == nil {
		return
	}
	select {
	case s.resultChan <- locationRecord:
	case <-time.After(s.resultTimeout):
		s.log.Warn(writeResultTimeout)
	}
}

func (s *objectLocationDetector) processRoutine(ctx context.Context, taskChan <-chan Address) {
loop:
	for {
		select {
		case <-ctx.Done():
			s.log.Warn(locationDetectorEntity+ctxDoneMsg, zap.Error(ctx.Err()))
			break loop
		case addr, ok := <-taskChan:
			if !ok {
				s.log.Warn(locationDetectorEntity + taskChanClosed)
				break loop
			} else if has, err := s.presenceChecker.Has(addr); err != nil || !has {
				continue loop
			}
			s.handleTask(ctx, addr)
		}
	}
	close(s.resultChan)
}

func (s *objectLocationDetector) handleTask(ctx context.Context, addr Address) {
	var (
		err            error
		log            = s.log.With(addressFields(addr)...)
		locationRecord = &ObjectLocationRecord{addr, 0, nil}
	)

	if locationRecord.ReservationRatio, err = s.reservationRatioReceiver.ReservationRatio(ctx, addr); err != nil {
		log.Error("reservation ratio computation failure", zap.Error(err))
		return
	}

	nodes, err := s.objectLocator.LocateObject(ctx, addr)
	if err != nil {
		log.Error("locate object failure", zap.Error(err))
		return
	}

	for i := range nodes {
		locationRecord.Locations = append(locationRecord.Locations, ObjectLocation{
			Node:          nodes[i],
			WeightGreater: s.weightComparator.CompareWeight(ctx, addr, nodes[i]) == 1,
		})
	}

	log.Debug("current location record created",
		zap.Int("reservation ratio", locationRecord.ReservationRatio),
		zap.Any("storage nodes exclude self", locationRecord.Locations))

	s.writeResult(locationRecord)
}

// NewLocationDetector is an object location detector's constructor.
func NewLocationDetector(p *LocationDetectorParams) (ObjectLocationDetector, error) {
	switch {
	case p.PresenceChecker == nil:
		return nil, instanceError(locationDetectorEntity, presenceCheckerPart)
	case p.ObjectLocator == nil:
		return nil, instanceError(locationDetectorEntity, objectLocatorPart)
	case p.ReservationRatioReceiver == nil:
		return nil, instanceError(locationDetectorEntity, reservationRatioReceiverPart)
	case p.Logger == nil:
		return nil, instanceError(locationDetectorEntity, loggerPart)
	case p.WeightComparator == nil:
		return nil, instanceError(locationDetectorEntity, weightComparatorPart)
	}

	if p.TaskChanCap <= 0 {
		p.TaskChanCap = defaultLocationDetectorChanCap
	}

	if p.ResultTimeout <= 0 {
		p.ResultTimeout = defaultLocationDetectorResultTimeout
	}

	return &objectLocationDetector{
		weightComparator:         p.WeightComparator,
		objectLocator:            p.ObjectLocator,
		reservationRatioReceiver: p.ReservationRatioReceiver,
		presenceChecker:          p.PresenceChecker,
		log:                      p.Logger,
		taskChanCap:              p.TaskChanCap,
		resultTimeout:            p.ResultTimeout,
		resultChan:               nil,
	}, nil
}
