package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

type (
	// ContractProcessor interface defines functions for binding event producers
	// such as event.Listener and Timers with contract processor.
	ContractProcessor interface {
		ListenerNotificationParsers() []event.NotificationParserInfo
		ListenerNotificationHandlers() []event.NotificationHandlerInfo
		TimersHandlers() []event.NotificationHandlerInfo
	}
)

func connectListenerWithProcessor(l event.Listener, p ContractProcessor) {
	// register parsers
	for _, parser := range p.ListenerNotificationParsers() {
		l.SetNotificationParser(parser)
	}

	// register handlers
	for _, handler := range p.ListenerNotificationHandlers() {
		l.RegisterNotificationHandler(handler)
	}
}

// bindMorphProcessor connects morph chain listener handlers.
func bindMorphProcessor(proc ContractProcessor, s *Server) error {
	connectListenerWithProcessor(s.morphListener, proc)
	return nil
}

// bindMainnetProcessor connects mainnet chain listener handlers.
func bindMainnetProcessor(proc ContractProcessor, s *Server) error {
	connectListenerWithProcessor(s.mainnetListener, proc)
	return nil
}
