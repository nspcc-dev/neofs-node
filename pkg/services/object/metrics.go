package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	MetricCollector struct {
		next ServiceServer
	}
)

const (
	namespace = "neofs_node"
	subsystem = "object"
)

// Request counter metrics.
var (
	getCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   namespace,
		Subsystem:   subsystem,
		Name:        "get_req_count",
		Help:        "Number of get request processed",
		ConstLabels: nil,
	})

	putCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "put_req_count",
		Help:      "Number of put request processed",
	})

	headCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "head_req_count",
		Help:      "Number of head request processed",
	})

	searchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "search_req_count",
		Help:      "Number of search request processed",
	})

	deleteCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "delete_req_count",
		Help:      "Number of delete request processed",
	})

	rangeCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "range_req_count",
		Help:      "Number of range request processed",
	})

	rangeHashCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "range_hash_req_count",
		Help:      "Number of range hash request processed",
	})
)

func registerMetrics() {
	prometheus.MustRegister(getCounter) // todo: replace with for loop over map
	prometheus.MustRegister(putCounter)
	prometheus.MustRegister(headCounter)
	prometheus.MustRegister(searchCounter)
	prometheus.MustRegister(deleteCounter)
	prometheus.MustRegister(rangeCounter)
	prometheus.MustRegister(rangeHashCounter)
}

func NewMetricCollector(next ServiceServer) *MetricCollector {
	registerMetrics()

	return &MetricCollector{
		next: next,
	}
}

func (m MetricCollector) Get(req *object.GetRequest, stream GetObjectStream) error {
	defer func() {
		getCounter.Inc()
	}()

	return m.next.Get(req, stream)
}

func (m MetricCollector) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	defer func() {
		putCounter.Inc()
	}()

	return m.next.Put(ctx)
}

func (m MetricCollector) Head(ctx context.Context, request *object.HeadRequest) (*object.HeadResponse, error) {
	defer func() {
		headCounter.Inc()
	}()

	return m.next.Head(ctx, request)
}

func (m MetricCollector) Search(req *object.SearchRequest, stream SearchStream) error {
	defer func() {
		searchCounter.Inc()
	}()

	return m.next.Search(req, stream)
}

func (m MetricCollector) Delete(ctx context.Context, request *object.DeleteRequest) (*object.DeleteResponse, error) {
	defer func() {
		deleteCounter.Inc()
	}()

	return m.next.Delete(ctx, request)
}

func (m MetricCollector) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	defer func() {
		rangeCounter.Inc()
	}()

	return m.next.GetRange(req, stream)
}

func (m MetricCollector) GetRangeHash(ctx context.Context, request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	defer func() {
		rangeHashCounter.Inc()
	}()

	return m.next.GetRangeHash(ctx, request)
}
