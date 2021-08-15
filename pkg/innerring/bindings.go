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

	// NotaryContractProcessor interface defines function for binding notary event
	// producers such as event.Listener with contract processor.
	//
	// This interface is optional for contract processor. If contract processor
	// supports notary event handling, it should implement both ContractProcessor
	// and NotaryContractProcessor interfaces.
	NotaryContractProcessor interface {
		ListenerNotaryParsers() []event.NotaryParserInfo
		ListenerNotaryHandlers() []event.NotaryHandlerInfo
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

	// add notary handlers if processor supports it
	if notaryProcessor, ok := p.(NotaryContractProcessor); ok {
		for _, notaryParser := range notaryProcessor.ListenerNotaryParsers() {
			l.SetNotaryParser(notaryParser)
		}

		for _, notaryHandler := range notaryProcessor.ListenerNotaryHandlers() {
			l.RegisterNotaryHandler(notaryHandler)
		}
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
