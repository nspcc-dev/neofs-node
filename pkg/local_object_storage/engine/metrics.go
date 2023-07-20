package engine

import (
	"time"
)

type MetricRegister interface {
	AddListContainersDuration(d time.Duration)
	AddEstimateContainerSizeDuration(d time.Duration)
	AddDeleteDuration(d time.Duration)
	AddExistsDuration(d time.Duration)
	AddGetDuration(d time.Duration)
	AddHeadDuration(d time.Duration)
	AddInhumeDuration(d time.Duration)
	AddPutDuration(d time.Duration)
	AddRangeDuration(d time.Duration)
	AddSearchDuration(d time.Duration)
	AddListObjectsDuration(d time.Duration)

	SetObjectCounter(shardID, objectType string, v uint64)
	AddToObjectCounter(shardID, objectType string, delta int)

	SetReadonly(shardID string, readonly bool)

	AddToContainerSize(cnrID string, size int64)
}

func elapsed(addFunc func(d time.Duration)) func() {
	t := time.Now()

	return func() {
		addFunc(time.Since(t))
	}
}
