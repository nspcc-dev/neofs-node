package subscriber

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

type (
	// Subscriber is an interface of the NotificationEvent listener.
	Subscriber interface {
		SubscribeForNotification(...util.Uint160) (<-chan *result.NotificationEvent, error)
		UnsubscribeForNotification()
		Close()
	}

	subscriber struct {
		*sync.RWMutex
		log    *zap.Logger
		client *client.WSClient

		notify    chan *result.NotificationEvent
		notifyIDs map[util.Uint160]string
	}

	// Params is a group of Subscriber constructor parameters.
	Params struct {
		Log         *zap.Logger
		Endpoint    string
		DialTimeout time.Duration
	}
)

var (
	errNilParams = errors.New("chain/subscriber: config was not provided to the constructor")

	errNilLogger = errors.New("chain/subscriber: logger was not provided to the constructor")
)

func (s *subscriber) SubscribeForNotification(contracts ...util.Uint160) (<-chan *result.NotificationEvent, error) {
	s.Lock()
	defer s.Unlock()

	notifyIDs := make(map[util.Uint160]string, len(contracts))

	for i := range contracts {
		// do not subscribe to already subscribed contracts
		if _, ok := s.notifyIDs[contracts[i]]; ok {
			continue
		}

		// subscribe to contract notifications
		id, err := s.client.SubscribeForExecutionNotifications(&contracts[i], nil)
		if err != nil {
			// if there is some error, undo all subscriptions and return error
			for _, id := range notifyIDs {
				_ = s.client.Unsubscribe(id)
			}

			return nil, err
		}

		// save notification id
		notifyIDs[contracts[i]] = id
	}

	// update global map of subscribed contracts
	for contract, id := range notifyIDs {
		s.notifyIDs[contract] = id
	}

	return s.notify, nil
}

func (s *subscriber) UnsubscribeForNotification() {
	s.Lock()
	defer s.Unlock()

	for i := range s.notifyIDs {
		err := s.client.Unsubscribe(s.notifyIDs[i])
		if err != nil {
			s.log.Error("unsubscribe for notification",
				zap.String("event", s.notifyIDs[i]),
				zap.Error(err))
		}

		delete(s.notifyIDs, i)
	}
}

func (s *subscriber) Close() {
	s.client.Close()
}

func (s *subscriber) routeNotifications(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case notification := <-s.client.Notifications:
			switch notification.Type {
			case response.NotificationEventID:
				notification, ok := notification.Value.(*result.NotificationEvent)
				if !ok {
					s.log.Error("can't cast notify event to the notify struct")
					continue
				}

				s.notify <- notification
			default:
				s.log.Debug("unsupported notification from the chain",
					zap.Uint8("type", uint8(notification.Type)),
				)
			}
		}
	}
}

// New is a constructs Neo:Morph event listener and returns Subscriber interface.
func New(ctx context.Context, p *Params) (Subscriber, error) {
	switch {
	case p == nil:
		return nil, errNilParams
	case p.Log == nil:
		return nil, errNilLogger
	}

	wsClient, err := client.NewWS(ctx, p.Endpoint, client.Options{
		DialTimeout: p.DialTimeout,
	})
	if err != nil {
		return nil, err
	}

	sub := &subscriber{
		RWMutex:   new(sync.RWMutex),
		log:       p.Log,
		client:    wsClient,
		notify:    make(chan *result.NotificationEvent),
		notifyIDs: make(map[util.Uint160]string),
	}

	// Worker listens all events from neo-go websocket and puts them
	// into corresponding channel. It may be notifications, transactions,
	// new blocks. For now only notifications.
	go sub.routeNotifications(ctx)

	return sub, nil
}
