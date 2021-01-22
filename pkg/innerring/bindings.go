package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

type (
	// ContractProcessor interface defines functions for binding event producers
	// such as event.Listener and Timers with contract processor.
	ContractProcessor interface {
		ListenerParsers() []event.ParserInfo
		ListenerHandlers() []event.HandlerInfo
		TimersHandlers() []event.HandlerInfo
	}
)

func connectListenerWithProcessor(l event.Listener, p ContractProcessor) {
	// register parsers
	for _, parser := range p.ListenerParsers() {
		l.SetParser(parser)
	}

	// register handlers
	for _, handler := range p.ListenerHandlers() {
		l.RegisterHandler(handler)
	}
}

// bindMorphProcessor connects both morph chain listener handlers and
// local timers handlers.
func bindMorphProcessor(proc ContractProcessor, s *Server) error {
	connectListenerWithProcessor(s.morphListener, proc)
	return nil
}

// bindMainnetProcessor connects both mainnet chain listener handlers and
// local timers handlers.
func bindMainnetProcessor(proc ContractProcessor, s *Server) error {
	connectListenerWithProcessor(s.mainnetListener, proc)
	return nil
}
