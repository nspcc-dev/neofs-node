package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const objectSubsystem = "object"

type (
	objectServiceMetrics struct {
		getCounter       prometheus.Counter
		putCounter       prometheus.Counter
		headCounter      prometheus.Counter
		searchCounter    prometheus.Counter
		deleteCounter    prometheus.Counter
		rangeCounter     prometheus.Counter
		rangeHashCounter prometheus.Counter

		getDuration       prometheus.Counter
		putDuration       prometheus.Counter
		headDuration      prometheus.Counter
		searchDuration    prometheus.Counter
		deleteDuration    prometheus.Counter
		rangeDuration     prometheus.Counter
		rangeHashDuration prometheus.Counter

		putPayload prometheus.Counter
		getPayload prometheus.Counter
	}
)

func newObjectServiceMetrics() objectServiceMetrics {
	var ( // Request counter metrics.
		getCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "get_req_count",
			Help:      "Number of get request processed",
		})

		putCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "put_req_count",
			Help:      "Number of put request processed",
		})

		headCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "head_req_count",
			Help:      "Number of head request processed",
		})

		searchCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "search_req_count",
			Help:      "Number of search request processed",
		})

		deleteCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "delete_req_count",
			Help:      "Number of delete request processed",
		})

		rangeCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "range_req_count",
			Help:      "Number of range request processed",
		})

		rangeHashCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: objectSubsystem,
			Name:      "range_hash_req_count",
			Help:      "Number of range hash request processed",
		})
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
	}
}

func (m objectServiceMetrics) register() {
	prometheus.MustRegister(m.getCounter)
	prometheus.MustRegister(m.putCounter)
	prometheus.MustRegister(m.headCounter)
	prometheus.MustRegister(m.searchCounter)
	prometheus.MustRegister(m.deleteCounter)
	prometheus.MustRegister(m.rangeCounter)
	prometheus.MustRegister(m.rangeHashCounter)

	prometheus.MustRegister(m.getDuration)
	prometheus.MustRegister(m.putDuration)
	prometheus.MustRegister(m.headDuration)
	prometheus.MustRegister(m.searchDuration)
	prometheus.MustRegister(m.deleteDuration)
	prometheus.MustRegister(m.rangeDuration)
	prometheus.MustRegister(m.rangeHashDuration)

	prometheus.MustRegister(m.putPayload)
	prometheus.MustRegister(m.getPayload)
}

func (m objectServiceMetrics) IncGetReqCounter() {
	m.getCounter.Inc()
}

func (m objectServiceMetrics) IncPutReqCounter() {
	m.putCounter.Inc()
}

func (m objectServiceMetrics) IncHeadReqCounter() {
	m.headCounter.Inc()
}

func (m objectServiceMetrics) IncSearchReqCounter() {
	m.searchCounter.Inc()
}

func (m objectServiceMetrics) IncDeleteReqCounter() {
	m.deleteCounter.Inc()
}

func (m objectServiceMetrics) IncRangeReqCounter() {
	m.rangeCounter.Inc()
}

func (m objectServiceMetrics) IncRangeHashReqCounter() {
	m.rangeHashCounter.Inc()
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
