package event

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"go.uber.org/zap"
)

// Listener is an interface of smart contract notification event listener.
type Listener interface {
	// Listen must start the event listener.
	//
	// Must listen to events with the parser installed.
	Listen(context.Context)

	// ListenWithError must start the event listener.
	//
	// Must listen to events with the parser installed.
	//
	// Must send error to channel if subscriber channel has been closed or
	// it could not be started.
	ListenWithError(context.Context, chan<- error)

	// SetNotificationParser must set the parser of particular contract event.
	//
	// Parser of each event must be set once. All parsers must be set before Listen call.
	//
	// Must ignore nil parsers and all calls after listener has been started.
	SetNotificationParser(NotificationParserInfo)

	// RegisterNotificationHandler must register the event handler for particular notification event of contract.
	//
	// The specified handler must be called after each capture and parsing of the event.
	//
	// Must ignore nil handlers.
	RegisterNotificationHandler(NotificationHandlerInfo)

	// RegisterBlockHandler must register chain block handler.
	//
	// The specified handler must be called after each capture and parsing of the new block from chain.
	//
	// Must ignore nil handlers.
	RegisterBlockHandler(BlockHandler)

	// Stop must stop the event listener.
	Stop()
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

	notificationParsers  map[scriptHashWithType]NotificationParser
	notificationHandlers map[scriptHashWithType][]Handler

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
func (l listener) Listen(ctx context.Context) {
	l.once.Do(func() {
		if err := l.listen(ctx, nil); err != nil {
			l.log.Error("could not start listen to events",
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
func (l listener) ListenWithError(ctx context.Context, intError chan<- error) {
	l.once.Do(func() {
		if err := l.listen(ctx, intError); err != nil {
			l.log.Error("could not start listen to events",
				zap.String("error", err.Error()),
			)
			intError <- err
		}
	})
}

func (l listener) listen(ctx context.Context, intError chan<- error) error {
	// create the list of listening contract hashes
	hashes := make([]util.Uint160, 0)

	// fill the list with the contracts with set event parsers.
	l.mtx.RLock()
	for hashType := range l.notificationParsers {
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
	l.started = true

	l.mtx.RUnlock()

	chEvent, err := l.subscriber.SubscribeForNotification(hashes...)
	if err != nil {
		return err
	}

	l.listenLoop(ctx, chEvent, intError)

	return nil
}

func (l listener) listenLoop(ctx context.Context, chEvent <-chan *state.NotificationEvent, intErr chan<- error) {
	var blockChan <-chan *block.Block

	if len(l.blockHandlers) > 0 {
		var err error
		if blockChan, err = l.subscriber.BlockNotifications(); err != nil {
			if intErr != nil {
				intErr <- fmt.Errorf("could not open block notifications channel: %w", err)
			} else {
				l.log.Debug("could not open block notifications channel",
					zap.String("error", err.Error()),
				)
			}

			return
		}
	} else {
		blockChan = make(chan *block.Block)
	}

loop:
	for {
		select {
		case <-ctx.Done():
			l.log.Info("stop event listener by context",
				zap.String("reason", ctx.Err().Error()),
			)
			break loop
		case notifyEvent, ok := <-chEvent:
			if !ok {
				l.log.Warn("stop event listener by channel")
				if intErr != nil {
					intErr <- errors.New("event subscriber connection has been terminated")
				}

				break loop
			} else if notifyEvent == nil {
				l.log.Warn("nil notification event was caught")
				continue loop
			}

			l.parseAndHandleNotification(notifyEvent)
		case b, ok := <-blockChan:
			if !ok {
				l.log.Warn("stop event listener by block channel")
				if intErr != nil {
					intErr <- errors.New("new block notification channel is closed")
				}

				break loop
			} else if b == nil {
				l.log.Warn("nil block was caught")
				continue loop
			}

			// TODO: consider asynchronous execution
			for i := range l.blockHandlers {
				l.blockHandlers[i](b)
			}
		}
	}
}

func (l listener) parseAndHandleNotification(notifyEvent *state.NotificationEvent) {
	log := l.log.With(
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

	l.mtx.RLock()
	parser, ok := l.notificationParsers[keyEvent]
	l.mtx.RUnlock()

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
	l.mtx.RLock()
	handlers := l.notificationHandlers[keyEvent]
	l.mtx.RUnlock()

	if len(handlers) == 0 {
		log.Info("notification handlers for parsed notification event were not registered",
			zap.Any("event", event),
		)

		return
	}

	for _, handler := range handlers {
		handler(event)
	}
}

// SetNotificationParser sets the parser of particular contract event.
//
// Ignores nil and already set parsers.
// Ignores the parser if listener is started.
func (l listener) SetNotificationParser(p NotificationParserInfo) {
	log := l.log.With(
		zap.String("script hash LE", p.ScriptHash().StringLE()),
		zap.Stringer("event type", p.getType()),
	)

	parser := p.parser()
	if parser == nil {
		log.Info("ignore nil event parser")
		return
	}

	l.mtx.Lock()
	defer l.mtx.Unlock()

	// check if the listener was started
	if l.started {
		log.Warn("listener has been already started, ignore parser")
		return
	}

	// add event parser
	if _, ok := l.notificationParsers[p.scriptHashWithType]; !ok {
		l.notificationParsers[p.scriptHashWithType] = p.parser()
	}

	log.Info("registered new event parser")
}

// RegisterNotificationHandler registers the handler for particular notification event of contract.
//
// Ignores nil handlers.
// Ignores handlers of event without parser.
func (l listener) RegisterNotificationHandler(p NotificationHandlerInfo) {
	log := l.log.With(
		zap.String("script hash LE", p.ScriptHash().StringLE()),
		zap.Stringer("event type", p.GetType()),
	)

	handler := p.Handler()
	if handler == nil {
		log.Warn("ignore nil event handler")
		return
	}

	// check if parser was set
	l.mtx.RLock()
	_, ok := l.notificationParsers[p.scriptHashWithType]
	l.mtx.RUnlock()

	if !ok {
		log.Warn("ignore handler of event w/o parser")
		return
	}

	// add event handler
	l.mtx.Lock()
	l.notificationHandlers[p.scriptHashWithType] = append(
		l.notificationHandlers[p.scriptHashWithType],
		p.Handler(),
	)
	l.mtx.Unlock()

	log.Info("registered new event handler")
}

// Stop closes subscription channel with remote neo node.
func (l listener) Stop() {
	l.subscriber.Close()
}

func (l *listener) RegisterBlockHandler(handler BlockHandler) {
	if handler == nil {
		l.log.Warn("ignore nil block handler")
		return
	}

	l.blockHandlers = append(l.blockHandlers, handler)
}

// NewListener create the notification event listener instance and returns Listener interface.
func NewListener(p ListenerParams) (Listener, error) {
	switch {
	case p.Logger == nil:
		return nil, fmt.Errorf("%s: %w", newListenerFailMsg, errNilLogger)
	case p.Subscriber == nil:
		return nil, fmt.Errorf("%s: %w", newListenerFailMsg, errNilSubscriber)
	}

	return &listener{
		mtx:                  new(sync.RWMutex),
		once:                 new(sync.Once),
		notificationParsers:  make(map[scriptHashWithType]NotificationParser),
		notificationHandlers: make(map[scriptHashWithType][]Handler),
		log:                  p.Logger,
		subscriber:           p.Subscriber,
	}, nil
}
