package event

// Handler is an Event processing function.
type Handler func(Event)

// HandlerInfo is a structure that groups
// the parameters of the handler of particular
// contract event.
type HandlerInfo struct {
	scriptHashWithType

	h Handler
}

// SetHandler is an event handler setter.
func (s *HandlerInfo) SetHandler(v Handler) {
	s.h = v
}

func (s HandlerInfo) handler() Handler {
	return s.h
}
