package grpc

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

type (
	billingStream struct {
		grpc.ServerStream
		*grpc.StreamServerInfo

		input  int
		output int
		cid    string
	}

	cider interface {
		CID() refs.CID
	}
)

const (
	typeInput  = "input"
	typeOutput = "output"

	labelType      = "type"
	labelMethod    = "method"
	labelContainer = "container"
)

var (
	serviceBillingBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "neofs",
		Name:      "billing_bytes",
		Help:      "Count of bytes received / sent for method and container",
	}, []string{labelType, labelMethod, labelContainer})

	serviceBillingCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "neofs",
		Name:      "billing_calls",
		Help:      "Count of calls for api methods",
	}, []string{labelMethod, labelContainer})
)

func init() {
	// Register billing metrics
	prometheus.MustRegister(serviceBillingBytes)
	prometheus.MustRegister(serviceBillingCalls)
}

func getProtoSize(val interface{}) int {
	if msg, ok := val.(proto.Message); ok && msg != nil {
		return proto.Size(msg)
	}

	return 0
}

func getProtoContainer(val interface{}) string {
	if t, ok := val.(cider); ok && t != nil {
		return t.CID().String()
	}

	return ""
}

func (b *billingStream) RecvMsg(msg interface{}) error {
	err := b.ServerStream.RecvMsg(msg)
	b.input += getProtoSize(msg)

	if cid := getProtoContainer(msg); cid != "" {
		b.cid = cid
	}

	return err
}

func (b *billingStream) SendMsg(msg interface{}) error {
	b.output += getProtoSize(msg)

	return b.ServerStream.SendMsg(msg)
}

func (b *billingStream) report() {
	labels := prometheus.Labels{
		labelMethod:    b.FullMethod,
		labelContainer: b.cid,
	}

	serviceBillingCalls.With(labels).Inc()

	labels[labelType] = typeInput
	serviceBillingBytes.With(labels).Add(float64(b.input))

	labels[labelType] = typeOutput
	serviceBillingBytes.With(labels).Add(float64(b.output))
}

func streamBilling(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	stream := &billingStream{
		ServerStream:     ss,
		StreamServerInfo: info,
	}

	err := handler(srv, stream)

	stream.report()

	return err
}

func unaryBilling(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (res interface{}, err error) {
	input := getProtoSize(req)
	cid := getProtoContainer(req)

	labels := prometheus.Labels{
		labelMethod:    info.FullMethod,
		labelContainer: cid,
	}

	serviceBillingCalls.With(labels).Inc()

	if res, err = handler(ctx, req); err != nil {
		return
	}

	output := getProtoSize(res)

	labels[labelType] = typeInput
	serviceBillingBytes.With(labels).Add(float64(input))

	labels[labelType] = typeOutput
	serviceBillingBytes.With(labels).Add(float64(output))

	return
}
