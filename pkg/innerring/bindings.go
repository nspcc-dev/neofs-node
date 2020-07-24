package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
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

func connectTimerWithProcessor(t *timers.Timers, p ContractProcessor) error {
	var err error
	for _, parser := range p.TimersHandlers() {
		err = t.RegisterHandler(parser)
		if err != nil {
			return err
		}
	}

	return nil
}

// bindMorphProcessor connects both morph chain listener handlers and
// local timers handlers.
func bindMorphProcessor(proc ContractProcessor, s *Server) error {
	connectListenerWithProcessor(s.morphListener, proc)
	return connectTimerWithProcessor(s.localTimers, proc)
}

// bindMainnetProcessor connects both mainnet chain listener handlers and
// local timers handlers.
func bindMainnetProcessor(proc ContractProcessor, s *Server) error {
	connectListenerWithProcessor(s.mainnetListener, proc)
	return connectTimerWithProcessor(s.localTimers, proc)
}
