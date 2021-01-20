package event

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Listener is an interface of smart contract notification event listener.
type Listener interface {
	// Must start the event listener.
	//
	// Must listen to events with the parser installed.
	Listen(context.Context)

	// Must start the event listener.
	//
	// Must listen to events with the parser installed.
	//
	// Must send error to channel if subscriber channel has been closed or
	// it could not be started.
	ListenWithError(context.Context, chan<- error)

	// Must set the parser of particular contract event.
	//
	// Parser of each event must be set once. All parsers must be set before Listen call.
	//
	// Must ignore nil parsers and all calls after listener has been started.
	SetParser(ParserInfo)

	// Must register the event handler for particular notification event of contract.
	//
	// The specified handler must be called after each capture and parsing of the event
	//
	// Must ignore nil handlers.
	RegisterHandler(HandlerInfo)

	// Must stop the event listener.
	Stop()

	// Must register chain block handler.
	//
	// The specified handler must be called after each capture and parsing of the new block from chain.
	//
	// Must ignore nil handlers.
	RegisterBlockHandler(BlockHandler)
}

// ListenerParams is a group of parameters
// for Listener constructor.
type ListenerParams struct {
	Logger *zap.Logger

	Subscriber subscriber.Subscriber
}

type listener struct {
	mtx *sync.RWMutex

	once *sync.Once

	started bool

	parsers map[scriptHashWithType]Parser

	handlers map[scriptHashWithType][]Handler

	log *zap.Logger

	subscriber subscriber.Subscriber

	blockHandlers []BlockHandler
}

const newListenerFailMsg = "could not instantiate Listener"

var (
	errNilLogger = errors.New("nil logger")

	errNilSubscriber = errors.New("nil event subscriber")
)

// Listen starts the listening for events with registered handlers.
//
// Executes once, all subsequent calls do nothing.
//
// Returns an error if listener was already started.
func (s listener) Listen(ctx context.Context) {
	s.once.Do(func() {
		if err := s.listen(ctx, nil); err != nil {
			s.log.Error("could not start listen to events",
				zap.String("error", err.Error()),
			)
		}
	})
}

// ListenWithError starts the listening for events with registered handlers and
// passing error message to intError channel if subscriber channel has been closed.
//
// Executes once, all subsequent calls do nothing.
//
// Returns an error if listener was already started.
func (s listener) ListenWithError(ctx context.Context, intError chan<- error) {
	s.once.Do(func() {
		if err := s.listen(ctx, intError); err != nil {
			s.log.Error("could not start listen to events",
				zap.String("error", err.Error()),
			)
			intError <- err
		}
	})
}

func (s listener) listen(ctx context.Context, intError chan<- error) error {
	// create the list of listening contract hashes
	hashes := make([]util.Uint160, 0)

	// fill the list with the contracts with set event parsers.
	s.mtx.RLock()
	for hashType := range s.parsers {
		scHash := hashType.ScriptHash()

		// prevent repetitions
		for _, hash := range hashes {
			if hash.Equals(scHash) {
				continue
			}
		}

		hashes = append(hashes, hashType.ScriptHash())
	}

	// mark listener as started
	s.started = true

	s.mtx.RUnlock()

	chEvent, err := s.subscriber.SubscribeForNotification(hashes...)
	if err != nil {
		return err
	}

	s.listenLoop(ctx, chEvent, intError)

	return nil
}

func (s listener) listenLoop(ctx context.Context, chEvent <-chan *state.NotificationEvent, intErr chan<- error) {
	blockChan, err := s.subscriber.BlockNotifications()
	if err != nil {
		intErr <- errors.Wrap(err, "could not open block notifications channel")

		return
	}

loop:
	for {
		select {
		case <-ctx.Done():
			s.log.Info("stop event listener by context",
				zap.String("reason", ctx.Err().Error()),
			)
			break loop
		case notifyEvent, ok := <-chEvent:
			if !ok {
				s.log.Warn("stop event listener by channel")
				if intErr != nil {
					intErr <- errors.New("event subscriber connection has been terminated")
				}

				break loop
			} else if notifyEvent == nil {
				s.log.Warn("nil notification event was caught")
				continue loop
			}

			s.parseAndHandle(notifyEvent)
		case b, ok := <-blockChan:
			if !ok {
				s.log.Warn("stop event listener by block channel")
				if intErr != nil {
					intErr <- errors.New("new block notification channel is closed")
				}

				break loop
			} else if b == nil {
				s.log.Warn("nil block was caught")
				continue loop
			}

			// TODO: consider asynchronous execution
			for i := range s.blockHandlers {
				s.blockHandlers[i](b)
			}
		}
	}
}

func (s listener) parseAndHandle(notifyEvent *state.NotificationEvent) {
	log := s.log.With(
		zap.String("script hash LE", notifyEvent.ScriptHash.StringLE()),
	)

	// stack item must be an array of items
	arr, err := client.ArrayFromStackItem(notifyEvent.Item)
	if err != nil {
		log.Warn("stack item is not an array type",
			zap.String("error", err.Error()),
		)

		return
	} else if len(arr) == 0 {
		log.Warn("stack item array is empty")
		return
	}

	// calculate event type from bytes
	typEvent := TypeFromString(notifyEvent.Name)

	log = log.With(
		zap.String("event type", notifyEvent.Name),
	)

	// get the event parser
	keyEvent := scriptHashWithType{}
	keyEvent.SetScriptHash(notifyEvent.ScriptHash)
	keyEvent.SetType(typEvent)

	s.mtx.RLock()
	parser, ok := s.parsers[keyEvent]
	s.mtx.RUnlock()

	if !ok {
		log.Debug("event parser not set")

		return
	}

	// parse the notification event
	event, err := parser(arr)
	if err != nil {
		log.Warn("could not parse notification event",
			zap.String("error", err.Error()),
		)

		return
	}

	// handler the event
	s.mtx.RLock()
	handlers := s.handlers[keyEvent]
	s.mtx.RUnlock()

	if len(handlers) == 0 {
		log.Info("handlers for parsed notification event were not registered",
			zap.Any("event", event),
		)

		return
	}

	for _, handler := range handlers {
		handler(event)
	}
}

// SetParser sets the parser of particular contract event.
//
// Ignores nil and already set parsers.
// Ignores the parser if listener is started.
func (s listener) SetParser(p ParserInfo) {
	log := s.log.With(
		zap.String("script hash LE", p.ScriptHash().StringLE()),
		zap.Stringer("event type", p.getType()),
	)

	parser := p.parser()
	if parser == nil {
		log.Info("ignore nil event parser")
		return
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// check if the listener was started
	if s.started {
		log.Warn("listener has been already started, ignore parser")
		return
	}

	// add event parser
	if _, ok := s.parsers[p.scriptHashWithType]; !ok {
		s.parsers[p.scriptHashWithType] = p.parser()
	}

	log.Info("registered new event parser")
}

// RegisterHandler registers the handler for particular notification event of contract.
//
// Ignores nil handlers.
// Ignores handlers of event without parser.
func (s listener) RegisterHandler(p HandlerInfo) {
	log := s.log.With(
		zap.String("script hash LE", p.ScriptHash().StringLE()),
		zap.Stringer("event type", p.GetType()),
	)

	handler := p.Handler()
	if handler == nil {
		log.Warn("ignore nil event handler")
		return
	}

	// check if parser was set
	s.mtx.RLock()
	_, ok := s.parsers[p.scriptHashWithType]
	s.mtx.RUnlock()

	if !ok {
		log.Warn("ignore handler of event w/o parser")
		return
	}

	// add event handler
	s.mtx.Lock()
	s.handlers[p.scriptHashWithType] = append(
		s.handlers[p.scriptHashWithType],
		p.Handler(),
	)
	s.mtx.Unlock()

	log.Info("registered new event handler")
}

// Stop closes subscription channel with remote neo node.
func (s listener) Stop() {
	s.subscriber.Close()
}

func (s *listener) RegisterBlockHandler(handler BlockHandler) {
	if handler == nil {
		s.log.Warn("ignore nil block handler")
		return
	}

	s.blockHandlers = append(s.blockHandlers, handler)
}

// NewListener create the notification event listener instance and returns Listener interface.
func NewListener(p ListenerParams) (Listener, error) {
	switch {
	case p.Logger == nil:
		return nil, errors.Wrap(errNilLogger, newListenerFailMsg)
	case p.Subscriber == nil:
		return nil, errors.Wrap(errNilSubscriber, newListenerFailMsg)
	}

	return &listener{
		mtx:        new(sync.RWMutex),
		once:       new(sync.Once),
		parsers:    make(map[scriptHashWithType]Parser),
		handlers:   make(map[scriptHashWithType][]Handler),
		log:        p.Logger,
		subscriber: p.Subscriber,
	}, nil
}
