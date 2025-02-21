package event

import (
	"github.com/nspcc-dev/neo-go/pkg/core/block"
)

// Handler is an Event processing function.
type Handler func(Event)

// HeaderHandler is a chain header processing function.
type HeaderHandler func(*block.Header)

// NotificationHandlerInfo is a structure that groups
// the parameters of the handler of particular
// contract event.
type NotificationHandlerInfo struct {
	scriptHashWithType

	h Handler
}

// SetHandler is an event handler setter.
func (s *NotificationHandlerInfo) SetHandler(v Handler) {
	s.h = v
}

// Handler returns an event handler.
func (s NotificationHandlerInfo) Handler() Handler {
	return s.h
}

// NotaryHandlerInfo is a structure that groups
// the parameters of the handler of particular
// notary event.
type NotaryHandlerInfo struct {
	notaryRequestTypes

	h Handler
}

// SetHandler is an event handler setter.
func (nhi *NotaryHandlerInfo) SetHandler(v Handler) {
	nhi.h = v
}

// Handler returns an event handler.
func (nhi NotaryHandlerInfo) Handler() Handler {
	return nhi.h
}
