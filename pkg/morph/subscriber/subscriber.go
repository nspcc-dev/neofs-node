package subscriber

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"go.uber.org/zap"
)

type (
	// Subscriber is an interface of the NotificationEvent listener.
	Subscriber interface {
		SubscribeForNotification(...util.Uint160) (<-chan *state.ContainedNotificationEvent, error)
		UnsubscribeForNotification()
		BlockNotifications() (<-chan *block.Block, error)
		SubscribeForNotaryRequests(mainTXSigner util.Uint160) (<-chan *result.NotaryRequestEvent, error)
		Close()
	}

	subscriber struct {
		*sync.RWMutex
		log    *zap.Logger
		client *client.Client

		notifyChan chan *state.ContainedNotificationEvent

		blockChan chan *block.Block

		notaryChan chan *result.NotaryRequestEvent
	}

	// Params is a group of Subscriber constructor parameters.
	Params struct {
		Log            *zap.Logger
		StartFromBlock uint32
		Client         *client.Client
	}
)

var (
	errNilParams = errors.New("chain/subscriber: config was not provided to the constructor")

	errNilLogger = errors.New("chain/subscriber: logger was not provided to the constructor")

	errNilClient = errors.New("chain/subscriber: client was not provided to the constructor")
)

func (s *subscriber) SubscribeForNotification(contracts ...util.Uint160) (<-chan *state.ContainedNotificationEvent, error) {
	s.Lock()
	defer s.Unlock()

	notifyIDs := make(map[util.Uint160]struct{}, len(contracts))

	for i := range contracts {
		// subscribe to contract notifications
		err := s.client.SubscribeForExecutionNotifications(contracts[i])
		if err != nil {
			// if there is some error, undo all subscriptions and return error
			for hash := range notifyIDs {
				_ = s.client.UnsubscribeContract(hash)
			}

			return nil, err
		}

		// save notification id
		notifyIDs[contracts[i]] = struct{}{}
	}

	return s.notifyChan, nil
}

func (s *subscriber) UnsubscribeForNotification() {
	err := s.client.UnsubscribeAll()
	if err != nil {
		s.log.Error("unsubscribe for notification",
			zap.Error(err))
	}
}

func (s *subscriber) Close() {
	s.client.Close()
}

func (s *subscriber) BlockNotifications() (<-chan *block.Block, error) {
	if err := s.client.SubscribeForNewBlocks(); err != nil {
		return nil, fmt.Errorf("could not subscribe for new block events: %w", err)
	}

	return s.blockChan, nil
}

func (s *subscriber) SubscribeForNotaryRequests(mainTXSigner util.Uint160) (<-chan *result.NotaryRequestEvent, error) {
	if err := s.client.SubscribeForNotaryRequests(mainTXSigner); err != nil {
		return nil, fmt.Errorf("could not subscribe for notary request events: %w", err)
	}

	return s.notaryChan, nil
}

func (s *subscriber) routeNotifications(ctx context.Context) {
	notificationChan := s.client.NotificationChannel()

	for {
		select {
		case <-ctx.Done():
			return
		case notification, ok := <-notificationChan:
			if !ok {
				s.log.Warn("remote notification channel has been closed")
				close(s.notifyChan)
				close(s.blockChan)
				close(s.notaryChan)

				return
			}

			switch notification.Type {
			case neorpc.NotificationEventID:
				notifyEvent, ok := notification.Value.(*state.ContainedNotificationEvent)
				if !ok {
					s.log.Error("can't cast notify event value to the notify struct",
						zap.String("received type", fmt.Sprintf("%T", notification.Value)),
					)
					continue
				}

				s.log.Debug("new notification event from sidechain",
					zap.String("name", notifyEvent.Name),
				)

				s.notifyChan <- notifyEvent
			case neorpc.BlockEventID:
				b, ok := notification.Value.(*block.Block)
				if !ok {
					s.log.Error("can't cast block event value to block",
						zap.String("received type", fmt.Sprintf("%T", notification.Value)),
					)
					continue
				}

				s.blockChan <- b
			case neorpc.NotaryRequestEventID:
				notaryRequest, ok := notification.Value.(*result.NotaryRequestEvent)
				if !ok {
					s.log.Error("can't cast notify event value to the notary request struct",
						zap.String("received type", fmt.Sprintf("%T", notification.Value)),
					)
					continue
				}

				s.notaryChan <- notaryRequest
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
	case p.Client == nil:
		return nil, errNilClient
	}

	err := awaitHeight(p.Client, p.StartFromBlock)
	if err != nil {
		return nil, err
	}

	sub := &subscriber{
		RWMutex:    new(sync.RWMutex),
		log:        p.Log,
		client:     p.Client,
		notifyChan: make(chan *state.ContainedNotificationEvent),
		blockChan:  make(chan *block.Block),
		notaryChan: make(chan *result.NotaryRequestEvent),
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
func awaitHeight(cli *client.Client, startFrom uint32) error {
	if startFrom == 0 {
		return nil
	}

	height, err := cli.BlockCount()
	if err != nil {
		return fmt.Errorf("could not get block height: %w", err)
	}

	if height < startFrom {
		return fmt.Errorf("RPC block counter %d didn't reach expected height %d", height, startFrom)
	}

	return nil
}
