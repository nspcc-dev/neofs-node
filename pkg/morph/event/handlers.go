package event

import (
	"github.com/nspcc-dev/neo-go/pkg/core/block"
)

// Handler is an Event processing function.
type Handler func(Event)

// BlockHandler is a chain block processing function.
type BlockHandler func(*block.Block)

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
