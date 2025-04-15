package writecache

import "time"

type metricRegister interface {
	AddWCPutDuration(shardID string, d time.Duration)
	AddWCFlushSingleDuration(shardID string, d time.Duration)
	AddWCFlushBatchDuration(shardID string, d time.Duration)
	IncWCObjectCount(shardID string)
	DecWCObjectCount(shardID string)
	AddWCSize(shardID string, size uint64)
	SetWCSize(shardID string, size uint64)
}

type metricsWithID struct {
	id string
	mr metricRegister
}

func (m *metricsWithID) AddWCPutDuration(d time.Duration) {
	m.mr.AddWCPutDuration(m.id, d)
}

func (m *metricsWithID) AddWCFlushSingleDuration(d time.Duration) {
	m.mr.AddWCFlushSingleDuration(m.id, d)
}

func (m *metricsWithID) AddWCFlushBatchDuration(d time.Duration) {
	m.mr.AddWCFlushBatchDuration(m.id, d)
}

func (m *metricsWithID) IncWCObjectCount() {
	if m.mr != nil {
		m.mr.IncWCObjectCount(m.id)
	}
}

func (m *metricsWithID) DecWCObjectCount() {
	if m.mr != nil {
		m.mr.DecWCObjectCount(m.id)
	}
}

func (m *metricsWithID) AddWCSize(size uint64) {
	if m.mr != nil {
		m.mr.AddWCSize(m.id, size)
	}
}

func (m *metricsWithID) SetWCSize(size uint64) {
	if m.mr != nil {
		m.mr.SetWCSize(m.id, size)
	}
}

func elapsed(addFunc func(d time.Duration)) func() {
	t := time.Now()

	return func() {
		addFunc(time.Since(t))
	}
}
