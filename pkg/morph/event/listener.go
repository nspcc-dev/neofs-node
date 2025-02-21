package event

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"go.uber.org/zap"
)

// Listener is an interface of smart contract notification event listener.
type Listener interface {
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

	// EnableNotarySupport enables notary request listening. Passed hashes are
	// notary mainTX signer and local key account. In practice, it means that listener
	// will subscribe only for notary requests that are going to be paid with passed
	// mainTX hash.
	//
	// Must not be called after Listen or ListenWithError.
	EnableNotarySupport(mainSigner util.Uint160, localAcc util.Uint160, alphaKeys client.AlphabetKeys, bc BlockCounter)

	// SetNotaryParser must set the parser of particular notary request event.
	//
	// Parser of each event must be set once. All parsers must be set before Listen call.
	//
	// Must ignore nil parsers and all calls after listener has been started.
	//
	// Has no effect if EnableNotarySupport was not called before Listen or ListenWithError.
	SetNotaryParser(NotaryParserInfo)

	// RegisterNotaryHandler must register the event handler for particular notification event of contract.
	//
	// The specified handler must be called after each capture and parsing of the event.
	//
	// Must ignore nil handlers.
	//
	// Has no effect if EnableNotarySupport was not called before Listen or ListenWithError.
	RegisterNotaryHandler(NotaryHandlerInfo)

	// RegisterHeaderHandler must register chain header handler.
	//
	// The specified handler will be called after each capture and parsing of the new header from chain.
	//
	// Must ignore nil handlers.
	RegisterHeaderHandler(HeaderHandler)

	// Stop must stop the event listener.
	Stop()
}

// ListenerParams is a group of parameters
// for Listener constructor.
type ListenerParams struct {
	Logger *zap.Logger

	Client *client.Client
}

type listener struct {
	mtx sync.RWMutex

	startOnce, stopOnce sync.Once

	started bool

	notificationParsers  map[scriptHashWithType]NotificationParser
	notificationHandlers map[scriptHashWithType][]Handler

	listenNotary           bool
	notaryEventsPreparator preparator
	notaryParsers          map[notaryRequestTypes]NotaryParser
	notaryHandlers         map[notaryRequestTypes]Handler
	notaryMainTXSigner     util.Uint160 // filter for notary subscription

	log *zap.Logger

	cli *client.Client

	headerHandlers []HeaderHandler
}

const newListenerFailMsg = "could not instantiate Listener"

// notaryRoutineNum limits the number of concurrent notary request handlers.
const notaryRoutineNum = 10

var (
	errNilLogger = errors.New("nil logger")

	errNilSubscriber = errors.New("nil event client")
)

// ListenWithError starts the listening for events with registered handlers and
// passing error message to intError channel if subscriber channel has been closed.
//
// Executes once, all subsequent calls do nothing.
//
// Returns an error if listener was already started.
func (l *listener) ListenWithError(ctx context.Context, intError chan<- error) {
	l.startOnce.Do(func() {
		if err := l.listen(ctx); err != nil {
			intError <- err
		}
	})
}

func (l *listener) listen(ctx context.Context) error {
	// mark listener as started
	l.started = true

	ctx, cancel := context.WithCancelCause(ctx)
	notifyCh, headerCh, notaryCh := l.cli.Notifications()

	go l.subscribe(cancel)
	go l.listenForHeaders(ctx, cancel, headerCh)
	go l.listenForNotary(ctx, cancel, notaryCh)
	// We need to block this goroutine too
	l.listenForNotifications(ctx, cancel, notifyCh)
	err := context.Cause(ctx)
	if !errors.Is(err, context.Canceled) {
		// Some real cause.
		return err
	}
	// Upper-layer cancellation.
	return nil
}

func (l *listener) subscribe(cancel context.CancelCauseFunc) {
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
	l.mtx.RUnlock()

	err := l.cli.ReceiveExecutionNotifications(hashes)
	if err != nil {
		cancel(err)
		return
	}

	if len(l.headerHandlers) > 0 {
		if err = l.cli.ReceiveHeaders(); err != nil {
			cancel(err)
			return
		}
	}

	if l.listenNotary {
		if err = l.cli.ReceiveNotaryRequests(l.notaryMainTXSigner); err != nil {
			cancel(err)
			return
		}
	}
}

func (l *listener) listenForHeaders(ctx context.Context, cancel context.CancelCauseFunc, headerCh <-chan *block.Header) {
	for {
		select {
		case <-ctx.Done():
			l.log.Info("header listener stopped",
				zap.Error(context.Cause(ctx)))
			return
		case h, ok := <-headerCh:
			if !ok {
				l.log.Warn("header channel closed, stopping event listener")
				cancel(errors.New("header notification channel was closed"))
				return
			}

			for i := range l.headerHandlers {
				l.headerHandlers[i](h)
			}
		}
	}
}

func (l *listener) listenForNotary(ctx context.Context, cancel context.CancelCauseFunc, notaryCh <-chan *result.NotaryRequestEvent) {
	var secondaryCh = make(chan *result.NotaryRequestEvent)

	for range notaryRoutineNum {
		go func() {
			for notaryEvent := range secondaryCh {
				l.parseAndHandleNotary(notaryEvent)
			}
		}()
	}

loop:
	for {
		select {
		case <-ctx.Done():
			l.log.Info("notary listener stopped",
				zap.Error(context.Cause(ctx)))
			break loop
		case notaryEvent, ok := <-notaryCh:
			if !ok {
				l.log.Warn("notary channel closed, stopping event listener")
				cancel(errors.New("notary notification channel was closed"))
				break loop
			}

			secondaryCh <- notaryEvent
		}
	}
	close(secondaryCh)
}

func (l *listener) listenForNotifications(ctx context.Context, cancel context.CancelCauseFunc, notifyCh <-chan *state.ContainedNotificationEvent) {
	for {
		select {
		case <-ctx.Done():
			l.log.Info("notification listener stopped",
				zap.Error(context.Cause(ctx)))
			return
		case notifyEvent, ok := <-notifyCh:
			if !ok {
				l.log.Warn("notification channel closed, stopping event listener")
				cancel(errors.New("notification channel was closed"))
				return
			}

			l.parseAndHandleNotification(notifyEvent)
		}
	}
}

func (l *listener) parseAndHandleNotification(notifyEvent *state.ContainedNotificationEvent) {
	log := l.log.With(
		zap.String("script hash LE", notifyEvent.ScriptHash.StringLE()),
	)

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
	event, err := parser(notifyEvent)
	if err != nil {
		log.Warn("could not parse notification event",
			zap.Error(err),
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

func (l *listener) parseAndHandleNotary(nr *result.NotaryRequestEvent) {
	// prepare the notary event
	notaryEvent, err := l.notaryEventsPreparator.Prepare(nr.NotaryRequest)
	if err != nil {
		switch {
		case errors.Is(err, ErrTXAlreadyHandled) || errors.Is(err, ErrUnknownEvent):
		case errors.Is(err, ErrMainTXExpired):
			l.log.Warn("skip expired main TX notary event",
				zap.Error(err),
			)
		default:
			l.log.Warn("could not prepare and validate notary event",
				zap.Error(err),
			)
		}

		return
	}

	log := l.log.With(
		zap.String("contract", notaryEvent.ScriptHash().StringLE()),
		zap.Stringer("method", notaryEvent.Type()),
	)

	notaryKey := notaryRequestTypes{}
	notaryKey.SetRequestType(notaryEvent.Type())
	notaryKey.SetScriptHash(notaryEvent.ScriptHash())

	// get notary parser
	l.mtx.RLock()
	parser, ok := l.notaryParsers[notaryKey]
	l.mtx.RUnlock()

	if !ok {
		log.Debug("notary parser not set")

		return
	}

	// parse the notary event
	event, err := parser(notaryEvent)
	if err != nil {
		log.Warn("could not parse notary event",
			zap.Error(err),
		)

		return
	}

	// handle the event
	l.mtx.RLock()
	handler, ok := l.notaryHandlers[notaryKey]
	l.mtx.RUnlock()

	if !ok {
		log.Info("notary handlers for parsed notification event were not registered",
			zap.Any("event", event),
		)

		return
	}

	handler(event)
}

// SetNotificationParser sets the parser of particular contract event.
//
// Ignores nil and already set parsers.
// Ignores the parser if listener is started.
func (l *listener) SetNotificationParser(pi NotificationParserInfo) {
	log := l.log.With(
		zap.String("contract", pi.ScriptHash().StringLE()),
		zap.Stringer("event_type", pi.getType()),
	)

	parser := pi.parser()
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
	if _, ok := l.notificationParsers[pi.scriptHashWithType]; !ok {
		l.notificationParsers[pi.scriptHashWithType] = pi.parser()
	}

	log.Debug("registered new event parser")
}

// RegisterNotificationHandler registers the handler for particular notification event of contract.
//
// Ignores nil handlers.
// Ignores handlers of event without parser.
func (l *listener) RegisterNotificationHandler(hi NotificationHandlerInfo) {
	log := l.log.With(
		zap.String("contract", hi.ScriptHash().StringLE()),
		zap.Stringer("event_type", hi.GetType()),
	)

	handler := hi.Handler()
	if handler == nil {
		log.Warn("ignore nil event handler")
		return
	}

	// check if parser was set
	l.mtx.RLock()
	_, ok := l.notificationParsers[hi.scriptHashWithType]
	l.mtx.RUnlock()

	if !ok {
		log.Warn("ignore handler of event w/o parser")
		return
	}

	// add event handler
	l.mtx.Lock()
	l.notificationHandlers[hi.scriptHashWithType] = append(
		l.notificationHandlers[hi.scriptHashWithType],
		hi.Handler(),
	)
	l.mtx.Unlock()

	log.Debug("registered new event handler")
}

// EnableNotarySupport enables notary request listening. Passed hash is
// notary mainTX signer. In practise, it means that listener will subscribe
// for only notary requests that are going to be paid with passed hash.
//
// Must not be called after Listen or ListenWithError.
func (l *listener) EnableNotarySupport(mainTXSigner util.Uint160, localAcc util.Uint160, alphaKeys client.AlphabetKeys, bc BlockCounter) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	l.listenNotary = true
	l.notaryMainTXSigner = mainTXSigner
	l.notaryHandlers = make(map[notaryRequestTypes]Handler)
	l.notaryParsers = make(map[notaryRequestTypes]NotaryParser)
	l.notaryEventsPreparator = notaryPreparator(localAcc, alphaKeys, bc)
}

// SetNotaryParser sets the parser of particular notary request event.
//
// Ignores nil and already set parsers.
// Ignores the parser if listener is started.
func (l *listener) SetNotaryParser(pi NotaryParserInfo) {
	if !l.listenNotary {
		return
	}

	log := l.log.With(
		zap.String("contract", pi.ScriptHash().StringLE()),
		zap.Stringer("notary_type", pi.RequestType()),
	)

	parser := pi.parser()
	if parser == nil {
		log.Info("ignore nil notary event parser")
		return
	}

	l.mtx.Lock()
	defer l.mtx.Unlock()

	// check if the listener was started
	if l.started {
		log.Warn("listener has been already started, ignore notary parser")
		return
	}

	// add event parser
	if _, ok := l.notaryParsers[pi.notaryRequestTypes]; !ok {
		l.notaryParsers[pi.notaryRequestTypes] = pi.parser()
	}

	l.notaryEventsPreparator.allowNotaryEvent(pi.notaryScriptWithHash)

	log.Info("registered new event parser")
}

// RegisterNotaryHandler registers the handler for particular notification notary request event.
//
// Ignores nil handlers.
// Ignores handlers of event without parser.
func (l *listener) RegisterNotaryHandler(hi NotaryHandlerInfo) {
	if !l.listenNotary {
		return
	}

	log := l.log.With(
		zap.String("contract", hi.ScriptHash().StringLE()),
		zap.Stringer("notary type", hi.RequestType()),
	)

	handler := hi.Handler()
	if handler == nil {
		log.Warn("ignore nil notary event handler")
		return
	}

	// check if parser was set
	l.mtx.RLock()
	_, ok := l.notaryParsers[hi.notaryRequestTypes]
	l.mtx.RUnlock()

	if !ok {
		log.Warn("ignore handler of notary event w/o parser")
		return
	}

	// add notary event handler
	l.mtx.Lock()
	l.notaryHandlers[hi.notaryRequestTypes] = hi.Handler()
	l.mtx.Unlock()

	log.Info("registered new event handler")
}

// Stop closes subscription channel with remote neo node.
func (l *listener) Stop() {
	l.stopOnce.Do(func() {
		l.cli.Close()
	})
}

func (l *listener) RegisterHeaderHandler(handler HeaderHandler) {
	if handler == nil {
		l.log.Warn("ignore nil header handler")
		return
	}

	l.headerHandlers = append(l.headerHandlers, handler)
}

// NewListener create the notification event listener instance and returns Listener interface.
func NewListener(p ListenerParams) (Listener, error) {
	switch {
	case p.Logger == nil:
		return nil, fmt.Errorf("%s: %w", newListenerFailMsg, errNilLogger)
	case p.Client == nil:
		return nil, fmt.Errorf("%s: %w", newListenerFailMsg, errNilSubscriber)
	}

	return &listener{
		notificationParsers:  make(map[scriptHashWithType]NotificationParser),
		notificationHandlers: make(map[scriptHashWithType][]Handler),
		log:                  p.Logger,
		cli:                  p.Client,
	}, nil
}
