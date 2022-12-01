package object

import (
	"context"
	"time"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
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
		cid     string
	}

	putStreamMetric struct {
		stream  PutObjectStream
		metrics MetricRegister
		start   time.Time
		cid     string
	}

	MetricRegister interface {
		IncGetReqCounter(cnrID string, success bool)
		IncPutReqCounter(cnrID string, success bool)
		IncHeadReqCounter(cnrID string, success bool)
		IncSearchReqCounter(cnrID string, success bool)
		IncDeleteReqCounter(cnrID string, success bool)
		IncRangeReqCounter(cnrID string, success bool)
		IncRangeHashReqCounter(cnrID string, success bool)

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

func (m MetricCollector) Get(req *object.GetRequest, stream GetObjectStream) (err error) {
	t := time.Now()
	defer func() {
		cnr := getCIDString(req.GetBody().GetAddress().GetContainerID())
		m.metrics.IncGetReqCounter(cnr, err == nil)
		m.metrics.AddGetReqDuration(time.Since(t))
	}()

	err = m.next.Get(req, &getStreamMetric{
		ServerStream: stream,
		stream:       stream,
		metrics:      m.metrics,
	})
	return
}

func (m MetricCollector) Put(ctx context.Context) (PutObjectStream, error) {
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

func (m MetricCollector) Head(ctx context.Context, request *object.HeadRequest) (*object.HeadResponse, error) {
	t := time.Now()

	res, err := m.next.Head(ctx, request)

	cnr := getCIDString(request.GetBody().GetAddress().GetContainerID())
	m.metrics.IncHeadReqCounter(cnr, err == nil)
	m.metrics.AddHeadReqDuration(time.Since(t))

	return res, err
}

func (m MetricCollector) Search(req *object.SearchRequest, stream SearchStream) error {
	t := time.Now()

	err := m.next.Search(req, stream)

	cnr := getCIDString(req.GetBody().GetContainerID())
	m.metrics.IncSearchReqCounter(cnr, err == nil)
	m.metrics.AddSearchReqDuration(time.Since(t))

	return err
}

func (m MetricCollector) Delete(ctx context.Context, request *object.DeleteRequest) (*object.DeleteResponse, error) {
	t := time.Now()

	res, err := m.next.Delete(ctx, request)

	cnr := getCIDString(request.GetBody().GetAddress().GetContainerID())
	m.metrics.IncDeleteReqCounter(cnr, err == nil)
	m.metrics.AddDeleteReqDuration(time.Since(t))

	return res, err
}

func (m MetricCollector) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	t := time.Now()

	err := m.next.GetRange(req, stream)

	cnr := getCIDString(req.GetBody().GetAddress().GetContainerID())
	m.metrics.IncRangeReqCounter(cnr, err == nil)
	m.metrics.AddRangeReqDuration(time.Since(t))

	return err
}

func (m MetricCollector) GetRangeHash(ctx context.Context, request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	t := time.Now()

	res, err := m.next.GetRangeHash(ctx, request)

	cnr := getCIDString(request.GetBody().GetAddress().GetContainerID())
	m.metrics.IncRangeHashReqCounter(cnr, err == nil)
	m.metrics.AddRangeHashReqDuration(time.Since(t))

	return res, err
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

	s.metrics.IncPutReqCounter(s.cid, err == nil)
	s.metrics.AddPutReqDuration(time.Since(s.start))

	return res, err
}

func getCIDString(cnrV2 *refs.ContainerID) string {
	// Must be equal to `EncodeToString` for container ID from SDK.
	return base58.Encode(cnrV2.GetValue())
}
