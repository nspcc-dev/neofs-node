package object

import (
	"context"
	"time"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	MetricCollector struct {
		next ServiceServer
	}

	getStreamMetric struct {
		util.ServerStream
		stream GetObjectStream
	}

	putStreamMetric struct {
		stream object.PutObjectStreamer
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

// Request duration metrics.
var (
	getDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "get_req_duration",
		Help:      "Accumulated get request process duration",
	})

	putDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "put_req_duration",
		Help:      "Accumulated put request process duration",
	})

	headDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "head_req_duration",
		Help:      "Accumulated head request process duration",
	})

	searchDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "search_req_duration",
		Help:      "Accumulated search request process duration",
	})

	deleteDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "delete_req_duration",
		Help:      "Accumulated delete request process duration",
	})

	rangeDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "range_req_duration",
		Help:      "Accumulated range request process duration",
	})

	rangeHashDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "range_hash_req_duration",
		Help:      "Accumulated range hash request process duration",
	})
)

// Object payload metrics.
var (
	putPayload = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "put_payload",
		Help:      "Accumulated payload size at object put method",
	})

	getPayload = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "get_payload",
		Help:      "Accumulated payload size at object get method",
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

	prometheus.MustRegister(getDuration)
	prometheus.MustRegister(putDuration)
	prometheus.MustRegister(headDuration)
	prometheus.MustRegister(searchDuration)
	prometheus.MustRegister(deleteDuration)
	prometheus.MustRegister(rangeDuration)
	prometheus.MustRegister(rangeHashDuration)

	prometheus.MustRegister(putPayload)
	prometheus.MustRegister(getPayload)
}

func NewMetricCollector(next ServiceServer) *MetricCollector {
	registerMetrics()

	return &MetricCollector{
		next: next,
	}
}

func (m MetricCollector) Get(req *object.GetRequest, stream GetObjectStream) error {
	t := time.Now()
	defer func() {
		getCounter.Inc()
		getDuration.Add(float64(time.Since(t)))
	}()

	return m.next.Get(req, &getStreamMetric{
		ServerStream: stream,
		stream:       stream,
	})
}

func (m MetricCollector) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	t := time.Now()
	defer func() {
		putCounter.Inc()
		putDuration.Add(float64(time.Since(t)))
	}()

	stream, err := m.next.Put(ctx)
	if err != nil {
		return nil, err
	}

	return &putStreamMetric{stream: stream}, nil
}

func (m MetricCollector) Head(ctx context.Context, request *object.HeadRequest) (*object.HeadResponse, error) {
	t := time.Now()
	defer func() {
		headCounter.Inc()
		headDuration.Add(float64(time.Since(t)))
	}()

	return m.next.Head(ctx, request)
}

func (m MetricCollector) Search(req *object.SearchRequest, stream SearchStream) error {
	t := time.Now()
	defer func() {
		searchCounter.Inc()
		searchDuration.Add(float64(time.Since(t)))
	}()

	return m.next.Search(req, stream)
}

func (m MetricCollector) Delete(ctx context.Context, request *object.DeleteRequest) (*object.DeleteResponse, error) {
	t := time.Now()
	defer func() {
		deleteCounter.Inc()
		deleteDuration.Add(float64(time.Since(t)))
	}()

	return m.next.Delete(ctx, request)
}

func (m MetricCollector) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	t := time.Now()
	defer func() {
		rangeCounter.Inc()
		rangeDuration.Add(float64(time.Since(t)))
	}()

	return m.next.GetRange(req, stream)
}

func (m MetricCollector) GetRangeHash(ctx context.Context, request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	t := time.Now()
	defer func() {
		rangeHashCounter.Inc()
		rangeHashDuration.Add(float64(time.Since(t)))
	}()

	return m.next.GetRangeHash(ctx, request)
}

func (s getStreamMetric) Send(resp *object.GetResponse) error {
	chunk, ok := resp.GetBody().GetObjectPart().(*object.GetObjectPartChunk)
	if ok {
		ln := len(chunk.GetChunk())
		getPayload.Add(float64(ln))
	}

	return s.stream.Send(resp)
}

func (s putStreamMetric) Send(req *object.PutRequest) error {
	chunk, ok := req.GetBody().GetObjectPart().(*object.PutObjectPartChunk)
	if ok {
		ln := len(chunk.GetChunk())
		putPayload.Add(float64(ln))
	}

	return s.stream.Send(req)
}

func (s putStreamMetric) CloseAndRecv() (*object.PutResponse, error) {
	return s.stream.CloseAndRecv()
}
