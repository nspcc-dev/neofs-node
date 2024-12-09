package event

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/panjf2000/ants/v2"
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

	Client *client.Client

	WorkerPoolCapacity int
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

	blockHandlers []BlockHandler

	pool *ants.Pool
}

const newListenerFailMsg = "could not instantiate Listener"

var (
	errNilLogger = errors.New("nil logger")

	errNilSubscriber = errors.New("nil event client")
)

// Listen starts the listening for events with registered handlers.
//
// Executes once, all subsequent calls do nothing.
//
// Returns an error if listener was already started.
func (l *listener) Listen(ctx context.Context) {
	l.startOnce.Do(func() {
		if err := l.listen(ctx); err != nil {
			l.log.Error("could not start listen to events",
				zap.Error(err),
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
func (l *listener) ListenWithError(ctx context.Context, intError chan<- error) {
	l.startOnce.Do(func() {
		if err := l.listen(ctx); err != nil {
			l.log.Error("could not start listen to events",
				zap.Error(err),
			)
			intError <- err
		}
	})
}

func (l *listener) listen(ctx context.Context) error {
	// mark listener as started
	l.started = true

	subErrCh := make(chan error)

	go l.subscribe(subErrCh)

	return l.listenLoop(ctx, subErrCh)
}

func (l *listener) subscribe(errCh chan error) {
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
		errCh <- fmt.Errorf("could not subscribe for notifications: %w", err)
		return
	}

	if len(l.blockHandlers) > 0 {
		if err = l.cli.ReceiveBlocks(); err != nil {
			errCh <- fmt.Errorf("could not subscribe for blocks: %w", err)
			return
		}
	}

	if l.listenNotary {
		if err = l.cli.ReceiveNotaryRequests(l.notaryMainTXSigner); err != nil {
			errCh <- fmt.Errorf("could not subscribe for notary requests: %w", err)
			return
		}
	}
}

func (l *listener) listenLoop(ctx context.Context, subErrCh chan error) error {
	nCh, bCh, notaryCh := l.cli.Notifications()
	var res error

loop:
	for {
		select {
		case res = <-subErrCh:
			l.log.Error("stop event listener by error", zap.Error(res))
			break loop
		case <-ctx.Done():
			l.log.Info("stop event listener by context",
				zap.String("reason", ctx.Err().Error()),
			)
			break loop
		case notifyEvent, ok := <-nCh:
			if !ok {
				l.log.Warn("stop event listener by notification channel")
				res = errors.New("event subscriber connection has been terminated")
				break loop
			}

			if err := l.pool.Submit(func() {
				l.parseAndHandleNotification(notifyEvent)
			}); err != nil {
				l.log.Warn("listener worker pool drained",
					zap.Int("capacity", l.pool.Cap()))
			}
		case notaryEvent, ok := <-notaryCh:
			if !ok {
				l.log.Warn("stop event listener by notary channel")
				res = errors.New("notary event subscriber connection has been terminated")
				break loop
			}

			if err := l.pool.Submit(func() {
				l.parseAndHandleNotary(notaryEvent)
			}); err != nil {
				l.log.Warn("listener worker pool drained",
					zap.Int("capacity", l.pool.Cap()))
			}
		case b, ok := <-bCh:
			if !ok {
				l.log.Warn("stop event listener by block channel")
				res = errors.New("new block notification channel is closed")
				break loop
			}

			if err := l.pool.Submit(func() {
				for i := range l.blockHandlers {
					l.blockHandlers[i](b)
				}
			}); err != nil {
				l.log.Warn("listener worker pool drained",
					zap.Int("capacity", l.pool.Cap()))
			}
		}
	}
	return res
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

func (l *listener) RegisterBlockHandler(handler BlockHandler) {
	if handler == nil {
		l.log.Warn("ignore nil block handler")
		return
	}

	l.blockHandlers = append(l.blockHandlers, handler)
}

// NewListener create the notification event listener instance and returns Listener interface.
func NewListener(p ListenerParams) (Listener, error) {
	// defaultPoolCap is a default worker
	// pool capacity if it was not specified
	// via params
	const defaultPoolCap = 100

	switch {
	case p.Logger == nil:
		return nil, fmt.Errorf("%s: %w", newListenerFailMsg, errNilLogger)
	case p.Client == nil:
		return nil, fmt.Errorf("%s: %w", newListenerFailMsg, errNilSubscriber)
	}

	poolCap := p.WorkerPoolCapacity
	if poolCap == 0 {
		poolCap = defaultPoolCap
	}

	pool, err := ants.NewPool(poolCap, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("could not init worker pool: %w", err)
	}

	return &listener{
		notificationParsers:  make(map[scriptHashWithType]NotificationParser),
		notificationHandlers: make(map[scriptHashWithType][]Handler),
		log:                  p.Logger,
		cli:                  p.Client,
		pool:                 pool,
	}, nil
}
