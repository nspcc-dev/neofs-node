package subscriber

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

type (
	// Subscriber is an interface of the NotificationEvent listener.
	Subscriber interface {
		SubscribeForNotification(...util.Uint160) (<-chan *state.NotificationEvent, error)
		UnsubscribeForNotification()
		Close()
		BlockNotifications() (<-chan *block.Block, error)
	}

	subscriber struct {
		*sync.RWMutex
		log    *zap.Logger
		client *client.WSClient

		notify    chan *state.NotificationEvent
		notifyIDs map[util.Uint160]string

		blockChan chan *block.Block
	}

	// Params is a group of Subscriber constructor parameters.
	Params struct {
		Log            *zap.Logger
		Endpoint       string
		DialTimeout    time.Duration
		RPCInitTimeout time.Duration
		StartFromBlock uint32
	}
)

var (
	errNilParams = errors.New("chain/subscriber: config was not provided to the constructor")

	errNilLogger = errors.New("chain/subscriber: logger was not provided to the constructor")
)

func (s *subscriber) SubscribeForNotification(contracts ...util.Uint160) (<-chan *state.NotificationEvent, error) {
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

func (s *subscriber) BlockNotifications() (<-chan *block.Block, error) {
	if _, err := s.client.SubscribeForNewBlocks(nil); err != nil {
		return nil, fmt.Errorf("could not subscribe for new block events: %w", err)
	}

	return s.blockChan, nil
}

func (s *subscriber) routeNotifications(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case notification, ok := <-s.client.Notifications:
			if !ok {
				s.log.Warn("remote channel has been closed")
				close(s.notify)
				close(s.blockChan)

				return
			}

			switch notification.Type {
			case response.NotificationEventID:
				notification, ok := notification.Value.(*state.NotificationEvent)
				if !ok {
					s.log.Error("can't cast notify event to the notify struct")
					continue
				}

				s.notify <- notification
			case response.BlockEventID:
				b, ok := notification.Value.(*block.Block)
				if !ok {
					s.log.Error("can't cast block event value to block")
					continue
				}

				s.blockChan <- b
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

	if err := wsClient.Init(); err != nil {
		return nil, fmt.Errorf("could not init ws client: %w", err)
	}

	p.Log.Debug("event subscriber awaits RPC node",
		zap.String("endpoint", p.Endpoint),
		zap.Uint32("min_block_height", p.StartFromBlock))

	err = awaitHeight(wsClient, p.StartFromBlock, p.RPCInitTimeout)
	if err != nil {
		return nil, err
	}

	sub := &subscriber{
		RWMutex:   new(sync.RWMutex),
		log:       p.Log,
		client:    wsClient,
		notify:    make(chan *state.NotificationEvent),
		notifyIDs: make(map[util.Uint160]string),
		blockChan: make(chan *block.Block),
	}

	// Worker listens all events from neo-go websocket and puts them
	// into corresponding channel. It may be notifications, transactions,
	// new blocks. For now only notifications.
	go sub.routeNotifications(ctx)

	return sub, nil
}

// awaitHeight checks if remote client has least expected block height and
// returns error if it is not reached that height after timeout duration.
// This function is required to avoid connections to unsynced RPC nodes, because
// they can produce events from the past that should not be processed by
// NeoFS nodes.
func awaitHeight(wsClient *client.WSClient, startFrom uint32, timeout time.Duration) error {
	if startFrom == 0 {
		return nil
	}

	for ch := time.After(timeout); ; {
		select {
		case <-ch:
			return fmt.Errorf("could not init ws client: didn't reach expected height %d", startFrom)
		default:
		}
		height, err := wsClient.GetBlockCount()
		if err != nil {
			return fmt.Errorf("could not get block height: %w", err)
		} else if height >= startFrom {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}
