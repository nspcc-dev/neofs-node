package object

import (
	"context"
	"time"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-sdk-go/stat"
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
		MetricCollector
		stream PutObjectStream
		start  time.Time
	}

	// MetricRegister tracks exec statistics for the following ops:
	//  - [stat.MethodObjectPut]
	//  - [stat.MethodObjectGet]
	//  - [stat.MethodObjectHead]
	//  - [stat.MethodObjectDelete]
	//  - [stat.MethodObjectSearch]
	//  - [stat.MethodObjectRange]
	//  - [stat.MethodObjectHash]
	MetricRegister interface {
		// HandleOpExecResult handles measured execution results of the given op.
		HandleOpExecResult(_ stat.Method, success bool, _ time.Duration)

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

func (m MetricCollector) pushOpExecResult(op stat.Method, err error, startedAt time.Time) {
	m.metrics.HandleOpExecResult(op, err == nil, time.Since(startedAt))
}

func (m MetricCollector) Get(req *object.GetRequest, stream GetObjectStream) (err error) {
	t := time.Now()
	defer func() {
		m.pushOpExecResult(stat.MethodObjectGet, err, t)
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
		stream:          stream,
		MetricCollector: m,
		start:           t,
	}, nil
}

func (m MetricCollector) Head(ctx context.Context, request *object.HeadRequest) (*object.HeadResponse, error) {
	t := time.Now()

	res, err := m.next.Head(ctx, request)

	m.pushOpExecResult(stat.MethodObjectHead, err, t)

	return res, err
}

func (m MetricCollector) Search(req *object.SearchRequest, stream SearchStream) error {
	t := time.Now()

	err := m.next.Search(req, stream)

	m.pushOpExecResult(stat.MethodObjectSearch, err, t)

	return err
}

func (m MetricCollector) Delete(ctx context.Context, request *object.DeleteRequest) (*object.DeleteResponse, error) {
	t := time.Now()

	res, err := m.next.Delete(ctx, request)

	m.pushOpExecResult(stat.MethodObjectDelete, err, t)

	return res, err
}

func (m MetricCollector) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	t := time.Now()

	err := m.next.GetRange(req, stream)

	m.pushOpExecResult(stat.MethodObjectRange, err, t)

	return err
}

func (m MetricCollector) GetRangeHash(ctx context.Context, request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	t := time.Now()

	res, err := m.next.GetRangeHash(ctx, request)

	m.pushOpExecResult(stat.MethodObjectHash, err, t)

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

	s.pushOpExecResult(stat.MethodObjectPut, err, s.start)

	return res, err
}
