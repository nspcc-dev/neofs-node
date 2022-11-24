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
		enabled bool
	}

	getStreamMetric struct {
		util.ServerStream
		stream  GetObjectStream
		metrics MetricRegister
	}

	putStreamMetric struct {
		stream  PutObjectStream
		metrics MetricRegister
		start   time.Time
	}

	MetricRegister interface {
		IncGetReqCounter(success bool)
		IncPutReqCounter(success bool)
		IncHeadReqCounter(success bool)
		IncSearchReqCounter(success bool)
		IncDeleteReqCounter(success bool)
		IncRangeReqCounter(success bool)
		IncRangeHashReqCounter(success bool)

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

func NewMetricCollector(next ServiceServer, register MetricRegister, enabled bool) *MetricCollector {
	return &MetricCollector{
		next:    next,
		metrics: register,
		enabled: enabled,
	}
}

func (m MetricCollector) Get(req *object.GetRequest, stream GetObjectStream) (err error) {
	if m.enabled {
		t := time.Now()
		defer func() {
			m.metrics.IncGetReqCounter(err == nil)
			m.metrics.AddGetReqDuration(time.Since(t))
		}()
		err = m.next.Get(req, &getStreamMetric{
			ServerStream: stream,
			stream:       stream,
			metrics:      m.metrics,
		})
	} else {
		err = m.next.Get(req, stream)
	}
	return
}

func (m MetricCollector) Put(ctx context.Context) (PutObjectStream, error) {
	if m.enabled {
		t := time.Now()

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
	return m.next.Put(ctx)
}

func (m MetricCollector) Head(ctx context.Context, request *object.HeadRequest) (*object.HeadResponse, error) {
	if m.enabled {
		t := time.Now()

		res, err := m.next.Head(ctx, request)

		m.metrics.IncHeadReqCounter(err == nil)
		m.metrics.AddHeadReqDuration(time.Since(t))

		return res, err
	}
	return m.next.Head(ctx, request)
}

func (m MetricCollector) Search(req *object.SearchRequest, stream SearchStream) error {
	if m.enabled {
		t := time.Now()

		err := m.next.Search(req, stream)

		m.metrics.IncSearchReqCounter(err == nil)
		m.metrics.AddSearchReqDuration(time.Since(t))

		return err
	}
	return m.next.Search(req, stream)
}

func (m MetricCollector) Delete(ctx context.Context, request *object.DeleteRequest) (*object.DeleteResponse, error) {
	if m.enabled {
		t := time.Now()

		res, err := m.next.Delete(ctx, request)

		m.metrics.IncDeleteReqCounter(err == nil)
		m.metrics.AddDeleteReqDuration(time.Since(t))
		return res, err
	}
	return m.next.Delete(ctx, request)
}

func (m MetricCollector) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	if m.enabled {
		t := time.Now()

		err := m.next.GetRange(req, stream)

		m.metrics.IncRangeReqCounter(err == nil)
		m.metrics.AddRangeReqDuration(time.Since(t))

		return err
	}
	return m.next.GetRange(req, stream)
}

func (m MetricCollector) GetRangeHash(ctx context.Context, request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	if m.enabled {
		t := time.Now()

		res, err := m.next.GetRangeHash(ctx, request)

		m.metrics.IncRangeHashReqCounter(err == nil)
		m.metrics.AddRangeHashReqDuration(time.Since(t))

		return res, err
	}
	return m.next.GetRangeHash(ctx, request)
}

func (m *MetricCollector) Enable() {
	m.enabled = true
}

func (m *MetricCollector) Disable() {
	m.enabled = false
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
	res, err := s.stream.CloseAndRecv()

	s.metrics.IncPutReqCounter(err == nil)
	s.metrics.AddPutReqDuration(time.Since(s.start))

	return res, err
}
