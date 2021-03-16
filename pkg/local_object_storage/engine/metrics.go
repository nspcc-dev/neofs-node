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
}

func elapsed(addFunc func(d time.Duration)) func() {
	t := time.Now()

	return func() {
		addFunc(time.Since(t))
	}
}
