package metrics

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-sdk-go/stat"
	"github.com/prometheus/client_golang/prometheus"
)

const objectSubsystem = "object"

type (
	methodCount struct {
		success prometheus.Counter
		total   prometheus.Counter
	}

	objectServiceMetrics struct {
		getCounter       methodCount
		putCounter       methodCount
		headCounter      methodCount
		searchCounter    methodCount
		deleteCounter    methodCount
		rangeCounter     methodCount
		rangeHashCounter methodCount

		getDuration       prometheus.Histogram
		putDuration       prometheus.Histogram
		headDuration      prometheus.Histogram
		searchDuration    prometheus.Histogram
		deleteDuration    prometheus.Histogram
		rangeDuration     prometheus.Histogram
		rangeHashDuration prometheus.Histogram

		putPayload prometheus.Counter
		getPayload prometheus.Counter

		shardMetrics   *prometheus.GaugeVec
		shardsReadonly *prometheus.GaugeVec
	}
)

const (
	shardIDLabelKey     = "shard"
	counterTypeLabelKey = "type"
	containerIDLabelKey = "cid"
)

func newMethodCallCounter(name string) methodCount {
	return methodCount{
		success: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      fmt.Sprintf("%s_req_count_success", name),
			Help:      fmt.Sprintf("The number of successful %s requests processed", name),
		}),
		total: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      fmt.Sprintf("%s_req_count", name),
			Help:      fmt.Sprintf("Total number of %s requests processed", name),
		}),
	}
}

func (m methodCount) mustRegister() {
	prometheus.MustRegister(m.success)
	prometheus.MustRegister(m.total)
}

func (m methodCount) inc(success bool) {
	m.total.Inc()
	if success {
		m.success.Inc()
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
		getDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "rpc_get_time",
			Help:      "RPC 'get' request handling time",
		})

		putDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "rpc_put_time",
			Help:      "RPC 'put' request handling time",
		})

		headDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "rpc_head_time",
			Help:      "RPC 'head' request handling time",
		})

		searchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "rpc_search_time",
			Help:      "RPC 'search' request handling time",
		})

		deleteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "rpc_delete_time",
			Help:      "RPC 'delete' request handling time",
		})

		rangeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "rpc_range_time",
			Help:      "RPC 'range request' handling time",
		})

		rangeHashDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "rpc_range_hash_time",
			Help:      "RPC 'range hash' handling time",
		})
	)

	var ( // Object payload metrics.
		putPayload = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "put_payload",
			Help:      "Accumulated payload size at object put method",
		})

		getPayload = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "get_payload",
			Help:      "Accumulated payload size at object get method",
		})

		shardsMetrics = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "counter",
			Help:      "Objects counters per shards",
		},
			[]string{shardIDLabelKey, counterTypeLabelKey},
		)

		shardsReadonly = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "readonly",
			Help:      "Shard state",
		},
			[]string{shardIDLabelKey},
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
		shardsReadonly:    shardsReadonly,
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
	prometheus.MustRegister(m.shardsReadonly)
}

func (m objectServiceMetrics) HandleOpExecResult(op stat.Method, success bool, d time.Duration) {
	switch op {
	default:
		panic(fmt.Sprintf("unsupported op %v", op))
	case stat.MethodObjectGet:
		m.getCounter.inc(success)
		m.getDuration.Observe(d.Seconds())
	case stat.MethodObjectPut:
		m.putCounter.inc(success)
		m.putDuration.Observe(d.Seconds())
	case stat.MethodObjectHead:
		m.headCounter.inc(success)
		m.headDuration.Observe(d.Seconds())
	case stat.MethodObjectDelete:
		m.deleteCounter.inc(success)
		m.deleteDuration.Observe(d.Seconds())
	case stat.MethodObjectSearch, stat.MethodObjectSearchV2: // FIXME: sep counters?
		m.searchCounter.inc(success)
		m.searchDuration.Observe(d.Seconds())
	case stat.MethodObjectRange:
		m.rangeCounter.inc(success)
		m.rangeDuration.Observe(d.Seconds())
	case stat.MethodObjectHash:
		m.rangeHashCounter.inc(success)
		m.rangeHashDuration.Observe(d.Seconds())
	}
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

func (m objectServiceMetrics) SetReadonly(shardID string, readonly bool) {
	var flag float64
	if readonly {
		flag = 1
	}
	m.shardsReadonly.With(
		prometheus.Labels{
			shardIDLabelKey: shardID,
		},
	).Set(flag)
}
