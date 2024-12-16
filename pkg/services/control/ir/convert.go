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

type notaryListResponseWrapper struct {
	m *NotaryListResponse
}

func (w *notaryListResponseWrapper) ToGRPCMessage() grpc.Message {
	return w.m
}

func (w *notaryListResponseWrapper) FromGRPCMessage(m grpc.Message) error {
	var ok bool

	w.m, ok = m.(*NotaryListResponse)
	if !ok {
		return message.NewUnexpectedMessageType(m, w.m)
	}

	return nil
}

type notaryRequestResponseWrapper struct {
	m *NotaryRequestResponse
}

func (w *notaryRequestResponseWrapper) ToGRPCMessage() grpc.Message {
	return w.m
}

func (w *notaryRequestResponseWrapper) FromGRPCMessage(m grpc.Message) error {
	var ok bool

	w.m, ok = m.(*NotaryRequestResponse)
	if !ok {
		return message.NewUnexpectedMessageType(m, w.m)
	}

	return nil
}

type notarySignResponseWrapper struct {
	m *NotarySignResponse
}

func (w *notarySignResponseWrapper) ToGRPCMessage() grpc.Message {
	return w.m
}

func (w *notarySignResponseWrapper) FromGRPCMessage(m grpc.Message) error {
	var ok bool

	w.m, ok = m.(*NotarySignResponse)
	if !ok {
		return message.NewUnexpectedMessageType(m, w.m)
	}

	return nil
}
