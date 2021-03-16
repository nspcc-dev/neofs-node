package object

import (
	"context"
	"time"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type (
	MetricCollector struct {
		next    ServiceServer
		metrics MetricRegister
	}

	getStreamMetric struct {
		util.ServerStream
		stream  GetObjectStream
		metrics MetricRegister
	}

	putStreamMetric struct {
		stream  object.PutObjectStreamer
		metrics MetricRegister
		start   time.Time
	}

	MetricRegister interface {
		IncGetReqCounter()
		IncPutReqCounter()
		IncHeadReqCounter()
		IncSearchReqCounter()
		IncDeleteReqCounter()
		IncRangeReqCounter()
		IncRangeHashReqCounter()

		AddGetReqDuration(time.Duration)
		AddPutReqDuration(time.Duration)
		AddHeadReqDuration(time.Duration)
		AddSearchReqDuration(time.Duration)
		AddDeleteReqDuration(time.Duration)
		AddRangeReqDuration(time.Duration)
		AddRangeHashReqDuration(time.Duration)

		AddPutPayload(int)
		AddGetPayload(int)
	}
)

func NewMetricCollector(next ServiceServer, register MetricRegister) *MetricCollector {
	return &MetricCollector{
		next:    next,
		metrics: register,
	}
}

func (m MetricCollector) Get(req *object.GetRequest, stream GetObjectStream) error {
	t := time.Now()
	defer func() {
		m.metrics.IncGetReqCounter()
		m.metrics.AddGetReqDuration(time.Since(t))
	}()

	return m.next.Get(req, &getStreamMetric{
		ServerStream: stream,
		stream:       stream,
		metrics:      m.metrics,
	})
}

func (m MetricCollector) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	t := time.Now()
	defer func() {
		m.metrics.IncPutReqCounter()
	}()

	stream, err := m.next.Put(ctx)
	if err != nil {
		return nil, err
	}

	return &putStreamMetric{
		stream:  stream,
		metrics: m.metrics,
		start:   t,
	}, nil
}

func (m MetricCollector) Head(ctx context.Context, request *object.HeadRequest) (*object.HeadResponse, error) {
	t := time.Now()
	defer func() {
		m.metrics.IncHeadReqCounter()
		m.metrics.AddHeadReqDuration(time.Since(t))
	}()

	return m.next.Head(ctx, request)
}

func (m MetricCollector) Search(req *object.SearchRequest, stream SearchStream) error {
	t := time.Now()
	defer func() {
		m.metrics.IncSearchReqCounter()
		m.metrics.AddSearchReqDuration(time.Since(t))
	}()

	return m.next.Search(req, stream)
}

func (m MetricCollector) Delete(ctx context.Context, request *object.DeleteRequest) (*object.DeleteResponse, error) {
	t := time.Now()
	defer func() {
		m.metrics.IncDeleteReqCounter()
		m.metrics.AddDeleteReqDuration(time.Since(t))
	}()

	return m.next.Delete(ctx, request)
}

func (m MetricCollector) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	t := time.Now()
	defer func() {
		m.metrics.IncRangeReqCounter()
		m.metrics.AddRangeReqDuration(time.Since(t))
	}()

	return m.next.GetRange(req, stream)
}

func (m MetricCollector) GetRangeHash(ctx context.Context, request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	t := time.Now()
	defer func() {
		m.metrics.IncRangeHashReqCounter()
		m.metrics.AddRangeHashReqDuration(time.Since(t))
	}()

	return m.next.GetRangeHash(ctx, request)
}

func (s getStreamMetric) Send(resp *object.GetResponse) error {
	chunk, ok := resp.GetBody().GetObjectPart().(*object.GetObjectPartChunk)
	if ok {
		s.metrics.AddGetPayload(len(chunk.GetChunk()))
	}

	return s.stream.Send(resp)
}

func (s putStreamMetric) Send(req *object.PutRequest) error {
	chunk, ok := req.GetBody().GetObjectPart().(*object.PutObjectPartChunk)
	if ok {
		s.metrics.AddPutPayload(len(chunk.GetChunk()))
	}

	return s.stream.Send(req)
}

func (s putStreamMetric) CloseAndRecv() (*object.PutResponse, error) {
	defer func() {
		s.metrics.AddPutReqDuration(time.Since(s.start))
	}()

	return s.stream.CloseAndRecv()
}
