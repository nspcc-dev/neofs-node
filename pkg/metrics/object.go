package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const objectSubsystem = "object"

type (
	methodCount struct {
		success prometheus.CounterVec
		total   prometheus.CounterVec
	}

	objectServiceMetrics struct {
		getCounter       methodCount
		putCounter       methodCount
		headCounter      methodCount
		searchCounter    methodCount
		deleteCounter    methodCount
		rangeCounter     methodCount
		rangeHashCounter methodCount

		getDuration       prometheus.Counter
		putDuration       prometheus.Counter
		headDuration      prometheus.Counter
		searchDuration    prometheus.Counter
		deleteDuration    prometheus.Counter
		rangeDuration     prometheus.Counter
		rangeHashDuration prometheus.Counter

		putPayload prometheus.Counter
		getPayload prometheus.Counter

		shardMetrics *prometheus.GaugeVec
	}
)

const (
	shardIDLabelKey     = "shard"
	counterTypeLabelKey = "type"
	containerIDLabelKey = "cid"
)

func newMethodCallCounter(name string) methodCount {
	return methodCount{
		success: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      fmt.Sprintf("%s_req_count", name),
			Help:      fmt.Sprintf("The number of successful %s requests processed", name),
		}, []string{containerIDLabelKey}),
		total: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      fmt.Sprintf("%s_req_count_success", name),
			Help:      fmt.Sprintf("Total number of %s requests processed", name),
		}, []string{containerIDLabelKey}),
	}
}

func (m methodCount) mustRegister() {
	prometheus.MustRegister(m.success)
	prometheus.MustRegister(m.total)
}

func (m methodCount) Inc(cnrID string, success bool) {
	labels := prometheus.Labels{containerIDLabelKey: cnrID}
	m.total.With(labels).Inc()
	if success {
		m.success.With(labels).Inc()
	}
}

func newObjectServiceMetrics() objectServiceMetrics {
	var ( // Request counter metrics.
		getCounter       = newMethodCallCounter("get")
		putCounter       = newMethodCallCounter("put")
		headCounter      = newMethodCallCounter("head")
		searchCounter    = newMethodCallCounter("search")
		deleteCounter    = newMethodCallCounter("delete")
		rangeCounter     = newMethodCallCounter("range")
		rangeHashCounter = newMethodCallCounter("range_hash")
	)

	var ( // Request duration metrics.
		getDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "get_req_duration",
			Help:      "Accumulated get request process duration",
		})

		putDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "put_req_duration",
			Help:      "Accumulated put request process duration",
		})

		headDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "head_req_duration",
			Help:      "Accumulated head request process duration",
		})

		searchDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "search_req_duration",
			Help:      "Accumulated search request process duration",
		})

		deleteDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "delete_req_duration",
			Help:      "Accumulated delete request process duration",
		})

		rangeDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "range_req_duration",
			Help:      "Accumulated range request process duration",
		})

		rangeHashDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "range_hash_req_duration",
			Help:      "Accumulated range hash request process duration",
		})
	)

	var ( // Object payload metrics.
		putPayload = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "put_payload",
			Help:      "Accumulated payload size at object put method",
		})

		getPayload = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "get_payload",
			Help:      "Accumulated payload size at object get method",
		})

		shardsMetrics = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "counter",
			Help:      "Objects counters per shards",
		},
			[]string{shardIDLabelKey, counterTypeLabelKey},
		)
	)

	return objectServiceMetrics{
		getCounter:        getCounter,
		putCounter:        putCounter,
		headCounter:       headCounter,
		searchCounter:     searchCounter,
		deleteCounter:     deleteCounter,
		rangeCounter:      rangeCounter,
		rangeHashCounter:  rangeHashCounter,
		getDuration:       getDuration,
		putDuration:       putDuration,
		headDuration:      headDuration,
		searchDuration:    searchDuration,
		deleteDuration:    deleteDuration,
		rangeDuration:     rangeDuration,
		rangeHashDuration: rangeHashDuration,
		putPayload:        putPayload,
		getPayload:        getPayload,
		shardMetrics:      shardsMetrics,
	}
}

func (m objectServiceMetrics) register() {
	m.getCounter.mustRegister()
	m.putCounter.mustRegister()
	m.headCounter.mustRegister()
	m.searchCounter.mustRegister()
	m.deleteCounter.mustRegister()
	m.rangeCounter.mustRegister()
	m.rangeHashCounter.mustRegister()

	prometheus.MustRegister(m.getDuration)
	prometheus.MustRegister(m.putDuration)
	prometheus.MustRegister(m.headDuration)
	prometheus.MustRegister(m.searchDuration)
	prometheus.MustRegister(m.deleteDuration)
	prometheus.MustRegister(m.rangeDuration)
	prometheus.MustRegister(m.rangeHashDuration)

	prometheus.MustRegister(m.putPayload)
	prometheus.MustRegister(m.getPayload)

	prometheus.MustRegister(m.shardMetrics)
}

func (m objectServiceMetrics) IncGetReqCounter(cnrID string, success bool) {
	m.getCounter.Inc(cnrID, success)
}

func (m objectServiceMetrics) IncPutReqCounter(cnrID string, success bool) {
	m.putCounter.Inc(cnrID, success)
}

func (m objectServiceMetrics) IncHeadReqCounter(cnrID string, success bool) {
	m.headCounter.Inc(cnrID, success)
}

func (m objectServiceMetrics) IncSearchReqCounter(cnrID string, success bool) {
	m.searchCounter.Inc(cnrID, success)
}

func (m objectServiceMetrics) IncDeleteReqCounter(cnrID string, success bool) {
	m.deleteCounter.Inc(cnrID, success)
}

func (m objectServiceMetrics) IncRangeReqCounter(cnrID string, success bool) {
	m.rangeCounter.Inc(cnrID, success)
}

func (m objectServiceMetrics) IncRangeHashReqCounter(cnrID string, success bool) {
	m.rangeHashCounter.Inc(cnrID, success)
}

func (m objectServiceMetrics) AddGetReqDuration(d time.Duration) {
	m.getDuration.Add(float64(d))
}

func (m objectServiceMetrics) AddPutReqDuration(d time.Duration) {
	m.putDuration.Add(float64(d))
}

func (m objectServiceMetrics) AddHeadReqDuration(d time.Duration) {
	m.headDuration.Add(float64(d))
}

func (m objectServiceMetrics) AddSearchReqDuration(d time.Duration) {
	m.searchDuration.Add(float64(d))
}

func (m objectServiceMetrics) AddDeleteReqDuration(d time.Duration) {
	m.deleteDuration.Add(float64(d))
}

func (m objectServiceMetrics) AddRangeReqDuration(d time.Duration) {
	m.rangeDuration.Add(float64(d))
}

func (m objectServiceMetrics) AddRangeHashReqDuration(d time.Duration) {
	m.rangeHashDuration.Add(float64(d))
}

func (m objectServiceMetrics) AddPutPayload(ln int) {
	m.putPayload.Add(float64(ln))
}

func (m objectServiceMetrics) AddGetPayload(ln int) {
	m.getPayload.Add(float64(ln))
}

func (m objectServiceMetrics) AddToObjectCounter(shardID, objectType string, delta int) {
	m.shardMetrics.With(
		prometheus.Labels{
			shardIDLabelKey:     shardID,
			counterTypeLabelKey: objectType,
		},
	).Add(float64(delta))
}

func (m objectServiceMetrics) SetObjectCounter(shardID, objectType string, v uint64) {
	m.shardMetrics.With(
		prometheus.Labels{
			shardIDLabelKey:     shardID,
			counterTypeLabelKey: objectType,
		},
	).Set(float64(v))
}
