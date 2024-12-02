package control

import (
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/message"
)

type requestWrapper struct {
	message.Message
	m grpc.Message
}

func (w *requestWrapper) ToGRPCMessage() grpc.Message {
	return w.m
}

type healthCheckResponseWrapper struct {
	m *HealthCheckResponse
}

func (w *healthCheckResponseWrapper) ToGRPCMessage() grpc.Message {
	return w.m
}

func (w *healthCheckResponseWrapper) FromGRPCMessage(m grpc.Message) error {
	var ok bool

	w.m, ok = m.(*HealthCheckResponse)
	if !ok {
		return message.NewUnexpectedMessageType(m, w.m)
	}

	return nil
}

type networkListResponseWrapper struct {
	m *NetworkListResponse
}

func (w *networkListResponseWrapper) ToGRPCMessage() grpc.Message {
	return w.m
}

func (w *networkListResponseWrapper) FromGRPCMessage(m grpc.Message) error {
	var ok bool

	w.m, ok = m.(*NetworkListResponse)
	if !ok {
		return message.NewUnexpectedMessageType(m, w.m)
	}

	return nil
}

type networkEpochTickResponseWrapper struct {
	m *NetworkEpochTickResponse
}

func (w *networkEpochTickResponseWrapper) ToGRPCMessage() grpc.Message {
	return w.m
}

func (w *networkEpochTickResponseWrapper) FromGRPCMessage(m grpc.Message) error {
	var ok bool

	w.m, ok = m.(*NetworkEpochTickResponse)
	if !ok {
		return message.NewUnexpectedMessageType(m, w.m)
	}

	return nil
}
