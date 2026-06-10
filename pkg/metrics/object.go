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
		getCounter    methodCount
		putCounter    methodCount
		headCounter   methodCount
		searchCounter methodCount
		deleteCounter methodCount
		rangeCounter  methodCount

		getDuration    prometheus.Histogram
		putDuration    prometheus.Histogram
		headDuration   prometheus.Histogram
		searchDuration prometheus.Histogram
		deleteDuration prometheus.Histogram
		rangeDuration  prometheus.Histogram

		putPayload prometheus.Counter
		getPayload prometheus.Counter

		shardMetrics   *prometheus.GaugeVec
		shardsReadonly *prometheus.GaugeVec

		putECPartNodeRetriesHistogram          prometheus.Histogram
		getECRecoveryCounter                   prometheus.Counter
		getECPartSuboptimalHistogram           prometheus.Histogram
		getECPartLocalStorageFailureCounter    prometheus.Counter
		getECPartLocalStorageFailure404Counter prometheus.Counter
		getECPartRemoteNodeFailureCounter      prometheus.Counter
		getECPartRemoteNodeFailure404Counter   prometheus.Counter
		getECPartInvalidLocalObjectCounter     prometheus.Counter
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
		getCounter    = newMethodCallCounter("get")
		putCounter    = newMethodCallCounter("put")
		headCounter   = newMethodCallCounter("head")
		searchCounter = newMethodCallCounter("search")
		deleteCounter = newMethodCallCounter("delete")
		rangeCounter  = newMethodCallCounter("range")
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
		getCounter:     getCounter,
		putCounter:     putCounter,
		headCounter:    headCounter,
		searchCounter:  searchCounter,
		deleteCounter:  deleteCounter,
		rangeCounter:   rangeCounter,
		getDuration:    getDuration,
		putDuration:    putDuration,
		headDuration:   headDuration,
		searchDuration: searchDuration,
		deleteDuration: deleteDuration,
		rangeDuration:  rangeDuration,
		putPayload:     putPayload,
		getPayload:     getPayload,
		shardMetrics:   shardsMetrics,
		shardsReadonly: shardsReadonly,

		putECPartNodeRetriesHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "put_ec_part_node_retries",
			Help:      "Number of suboptimal PUTs of EC parts to container nodes",
			Buckets:   []float64{1, 2, 3, 4, 5, 6, 7},
		}),
		getECRecoveryCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "get_ec_recovery_count",
			Help:      "Number of times when server could not collect EC object from data parts only",
		}),
		getECPartSuboptimalHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "get_ec_part_node_retries",
			Help:      "Number of times when server switched to reserve storage node to get EC part",
			Buckets:   []float64{1, 2, 3, 4, 5, 6, 7},
		}),
		getECPartLocalStorageFailureCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "get_ec_part_local_storage_failure_count",
			Help:      "Number of times when server could not receive EC part from local storage",
		}),
		getECPartLocalStorageFailure404Counter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "get_ec_part_local_storage_failure_404_count",
			Help:      "Number of times when server could not find EC part in local storage",
		}),
		getECPartRemoteNodeFailureCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "get_ec_part_remote_node_failure_count",
			Help:      "Number of times when server could not receive EC part from remote storage node",
		}),
		getECPartRemoteNodeFailure404Counter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "get_ec_part_remote_node_failure_404_count",
			Help:      "Number of times when server could not find EC part on remote storage node",
		}),
		getECPartInvalidLocalObjectCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: objectSubsystem,
			Name:      "get_ec_part_invalid_local_object_count",
			Help:      "Number of times when server could not handle EC part received from local storage",
		}),
	}
}

func (m objectServiceMetrics) register() {
	m.getCounter.mustRegister()
	m.putCounter.mustRegister()
	m.headCounter.mustRegister()
	m.searchCounter.mustRegister()
	m.deleteCounter.mustRegister()
	m.rangeCounter.mustRegister()

	prometheus.MustRegister(m.getDuration)
	prometheus.MustRegister(m.putDuration)
	prometheus.MustRegister(m.headDuration)
	prometheus.MustRegister(m.searchDuration)
	prometheus.MustRegister(m.deleteDuration)
	prometheus.MustRegister(m.rangeDuration)

	prometheus.MustRegister(m.putPayload)
	prometheus.MustRegister(m.getPayload)

	prometheus.MustRegister(m.shardMetrics)
	prometheus.MustRegister(m.shardsReadonly)

	prometheus.MustRegister(m.putECPartNodeRetriesHistogram)
	prometheus.MustRegister(m.getECRecoveryCounter)
	prometheus.MustRegister(m.getECPartSuboptimalHistogram)
	prometheus.MustRegister(m.getECPartLocalStorageFailureCounter)
	prometheus.MustRegister(m.getECPartLocalStorageFailure404Counter)
	prometheus.MustRegister(m.getECPartRemoteNodeFailureCounter)
	prometheus.MustRegister(m.getECPartRemoteNodeFailure404Counter)
	prometheus.MustRegister(m.getECPartInvalidLocalObjectCounter)
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

func (m objectServiceMetrics) SubmitPutECPartNodeRetries(retries uint32) {
	m.putECPartNodeRetriesHistogram.Observe(float64(retries))
}

func (m objectServiceMetrics) SubmitGetECRecovery() {
	m.getECRecoveryCounter.Inc()
}

func (m objectServiceMetrics) SubmitGetECPartNodeRetries(num uint32) {
	m.getECPartSuboptimalHistogram.Observe(float64(num))
}

func (m objectServiceMetrics) SubmitGetECPartLocalStorageFailure(notFound bool) {
	m.getECPartLocalStorageFailureCounter.Inc()
	if notFound {
		m.getECPartLocalStorageFailure404Counter.Inc()
	}
}

func (m objectServiceMetrics) SubmitGetECPartRemoteNodeFailure(notFound bool) {
	m.getECPartRemoteNodeFailureCounter.Inc()
	if notFound {
		m.getECPartRemoteNodeFailure404Counter.Inc()
	}
}

func (m objectServiceMetrics) SubmitGetECPartInvalidLocalObject() {
	m.getECPartInvalidLocalObjectCounter.Inc()
}
