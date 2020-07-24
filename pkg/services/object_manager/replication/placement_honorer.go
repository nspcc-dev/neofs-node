package replication

import (
	"context"
	"time"

	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type (
	// PlacementHonorer is an interface of entity
	// that listens tasks to piece out placement rule of container for particular object.
	PlacementHonorer interface {
		Process(ctx context.Context) chan<- *ObjectLocationRecord
		Subscribe(ch chan<- *ObjectLocationRecord)
	}

	placementHonorer struct {
		objectSource          ObjectSource
		objectReceptacle      ObjectReceptacle
		remoteStorageSelector RemoteStorageSelector
		presenceChecker       PresenceChecker
		log                   *zap.Logger

		taskChanCap   int
		resultTimeout time.Duration
		resultChan    chan<- *ObjectLocationRecord
	}

	// PlacementHonorerParams groups the parameters of placement honorer's constructor.
	PlacementHonorerParams struct {
		ObjectSource
		ObjectReceptacle
		RemoteStorageSelector
		PresenceChecker
		*zap.Logger

		TaskChanCap   int
		ResultTimeout time.Duration
	}
)

const (
	defaultPlacementHonorerChanCap       = 10
	defaultPlacementHonorerResultTimeout = time.Second
	placementHonorerEntity               = "placement honorer"
)

func (s *placementHonorer) Subscribe(ch chan<- *ObjectLocationRecord) { s.resultChan = ch }

func (s *placementHonorer) Process(ctx context.Context) chan<- *ObjectLocationRecord {
	ch := make(chan *ObjectLocationRecord, s.taskChanCap)
	go s.processRoutine(ctx, ch)

	return ch
}

func (s *placementHonorer) writeResult(locationRecord *ObjectLocationRecord) {
	if s.resultChan == nil {
		return
	}
	select {
	case s.resultChan <- locationRecord:
	case <-time.After(s.resultTimeout):
		s.log.Warn(writeResultTimeout)
	}
}

func (s *placementHonorer) processRoutine(ctx context.Context, taskChan <-chan *ObjectLocationRecord) {
loop:
	for {
		select {
		case <-ctx.Done():
			s.log.Warn(placementHonorerEntity+ctxDoneMsg, zap.Error(ctx.Err()))
			break loop
		case locationRecord, ok := <-taskChan:
			if !ok {
				s.log.Warn(placementHonorerEntity + taskChanClosed)
				break loop
			} else if has, err := s.presenceChecker.Has(locationRecord.Address); err != nil || !has {
				continue loop
			}
			s.handleTask(ctx, locationRecord)
		}
	}
	close(s.resultChan)
}

func (s *placementHonorer) handleTask(ctx context.Context, locationRecord *ObjectLocationRecord) {
	defer s.writeResult(locationRecord)

	var (
		err            error
		log            = s.log.With(addressFields(locationRecord.Address)...)
		copiesShortage = locationRecord.ReservationRatio - 1
		exclNodes      = make([]multiaddr.Multiaddr, 0)
		procLocations  []ObjectLocation
	)

	obj, err := s.objectSource.Get(ctx, locationRecord.Address)
	if err != nil {
		log.Warn("get object failure", zap.Error(err))
		return
	}

	tombstone := obj.IsTombstone()

	for copiesShortage > 0 {
		nodesInfo, err := s.remoteStorageSelector.SelectRemoteStorages(ctx, locationRecord.Address, exclNodes...)
		if err != nil {
			log.Warn("select remote storage nodes failure",
				zap.Stringer("object", locationRecord.Address),
				zap.Any("exclude nodes", exclNodes),
				zap.String("error", err.Error()),
			)

			return
		}

		if !tombstone {
			procLocations = make([]ObjectLocation, 0, len(nodesInfo))
		loop:
			for i := range nodesInfo {
				for j := range locationRecord.Locations {
					if locationRecord.Locations[j].Node.Equal(nodesInfo[i].Node) {
						copiesShortage--
						continue loop
					}
				}
				procLocations = append(procLocations, nodesInfo[i])
			}

			if len(procLocations) == 0 {
				return
			}
		} else {
			procLocations = nodesInfo
		}

		if err := s.objectReceptacle.Put(ctx, ObjectStoreParams{
			Object: obj,
			Nodes:  procLocations,
			Handler: func(loc ObjectLocation, success bool) {
				if success {
					copiesShortage--
					if tombstone {
						for i := range locationRecord.Locations {
							if locationRecord.Locations[i].Node.Equal(loc.Node) {
								return
							}
						}
					}
					locationRecord.Locations = append(locationRecord.Locations, loc)
				} else {
					exclNodes = append(exclNodes, loc.Node)
				}
			},
		}); err != nil {
			s.log.Warn("put object to new nodes failure", zap.Error(err))
			return
		}
	}
}

// NewPlacementHonorer is a placement honorer's constructor.
func NewPlacementHonorer(p PlacementHonorerParams) (PlacementHonorer, error) {
	switch {
	case p.RemoteStorageSelector == nil:
		return nil, instanceError(placementHonorerEntity, remoteStorageSelectorPart)
	case p.ObjectSource == nil:
		return nil, instanceError(placementHonorerEntity, objectSourcePart)
	case p.ObjectReceptacle == nil:
		return nil, instanceError(placementHonorerEntity, objectReceptaclePart)
	case p.Logger == nil:
		return nil, instanceError(placementHonorerEntity, loggerPart)
	case p.PresenceChecker == nil:
		return nil, instanceError(placementHonorerEntity, presenceCheckerPart)
	}

	if p.TaskChanCap <= 0 {
		p.TaskChanCap = defaultPlacementHonorerChanCap
	}

	if p.ResultTimeout <= 0 {
		p.ResultTimeout = defaultPlacementHonorerResultTimeout
	}

	return &placementHonorer{
		objectSource:          p.ObjectSource,
		objectReceptacle:      p.ObjectReceptacle,
		remoteStorageSelector: p.RemoteStorageSelector,
		presenceChecker:       p.PresenceChecker,
		log:                   p.Logger,
		taskChanCap:           p.TaskChanCap,
		resultTimeout:         p.ResultTimeout,
	}, nil
}
