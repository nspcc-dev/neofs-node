package subscriber

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
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

type (
	NotificationChannels struct {
		BlockCh          <-chan *block.Block
		NotificationsCh  <-chan *state.ContainedNotificationEvent
		NotaryRequestsCh <-chan *result.NotaryRequestEvent
	}

	// Subscriber is an interface of the NotificationEvent listener.
	Subscriber interface {
		SubscribeForNotification(...util.Uint160) error
		BlockNotifications() error
		SubscribeForNotaryRequests(mainTXSigner util.Uint160) error

		NotificationChannels() NotificationChannels

		Close()
	}

	subscriber struct {
		*sync.RWMutex
		log    *logger.Logger
		client *client.Client

		notifyChan chan *state.ContainedNotificationEvent
		blockChan  chan *block.Block
		notaryChan chan *result.NotaryRequestEvent

		curNotifyChan chan *state.ContainedNotificationEvent
		curBlockChan  chan *block.Block
		curNotaryChan chan *result.NotaryRequestEvent

		// cached subscription information
		subscribedEvents       map[util.Uint160]bool
		subscribedNotaryEvents map[util.Uint160]bool
		subscribedToNewBlocks  bool
	}

	// Params is a group of Subscriber constructor parameters.
	Params struct {
		Log            *logger.Logger
		StartFromBlock uint32
		Client         *client.Client
	}
)

func (s *subscriber) NotificationChannels() NotificationChannels {
	return NotificationChannels{
		BlockCh:          s.blockChan,
		NotificationsCh:  s.notifyChan,
		NotaryRequestsCh: s.notaryChan,
	}
}

var (
	errNilParams = errors.New("chain/subscriber: config was not provided to the constructor")

	errNilLogger = errors.New("chain/subscriber: logger was not provided to the constructor")

	errNilClient = errors.New("chain/subscriber: client was not provided to the constructor")
)

func (s *subscriber) SubscribeForNotification(contracts ...util.Uint160) error {
	s.Lock()
	defer s.Unlock()

	notifyIDs := make([]string, 0, len(contracts))

	for i := range contracts {
		if s.subscribedEvents[contracts[i]] {
			continue
		}
		// subscribe to contract notifications
		id, err := s.client.ReceiveExecutionNotifications(contracts[i], s.curNotifyChan)
		if err != nil {
			// if there is some error, undo all subscriptions and return error
			for _, id := range notifyIDs {
				_ = s.client.Unsubscribe(id)
			}

			return err
		}

		// save notification id
		notifyIDs = append(notifyIDs, id)
	}
	for i := range contracts {
		s.subscribedEvents[contracts[i]] = true
	}

	return nil
}

func (s *subscriber) Close() {
	s.client.Close()
}

func (s *subscriber) BlockNotifications() error {
	s.Lock()
	defer s.Unlock()
	if s.subscribedToNewBlocks {
		return nil
	}
	if _, err := s.client.ReceiveBlocks(s.curBlockChan); err != nil {
		return fmt.Errorf("could not subscribe for new block events: %w", err)
	}

	s.subscribedToNewBlocks = true

	return nil
}

func (s *subscriber) SubscribeForNotaryRequests(mainTXSigner util.Uint160) error {
	s.Lock()
	defer s.Unlock()
	if s.subscribedNotaryEvents[mainTXSigner] {
		return nil
	}
	if _, err := s.client.ReceiveNotaryRequests(mainTXSigner, s.curNotaryChan); err != nil {
		return fmt.Errorf("could not subscribe for notary request events: %w", err)
	}

	s.subscribedNotaryEvents[mainTXSigner] = true
	return nil
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

		curNotifyChan: make(chan *state.ContainedNotificationEvent),
		curBlockChan:  make(chan *block.Block),
		curNotaryChan: make(chan *result.NotaryRequestEvent),

		subscribedEvents:       make(map[util.Uint160]bool),
		subscribedNotaryEvents: make(map[util.Uint160]bool),
	}
	// Worker listens all events from temporary NeoGo channel and puts them
	// into corresponding permanent channels.
	go sub.routeNotifications(ctx)

	return sub, nil
}

func (s *subscriber) routeNotifications(ctx context.Context) {
	var (
		// TODO: not needed after nspcc-dev/neo-go#2980.
		cliCh             = s.client.NotificationChannel()
		restoreCh         chan bool
		restoreInProgress bool
	)

routeloop:
	for {
		var connLost bool
		s.RLock()
		notifCh := s.curNotifyChan
		blCh := s.curBlockChan
		notaryCh := s.curNotaryChan
		s.RUnlock()
		select {
		case <-ctx.Done():
			break routeloop
		case ev, ok := <-notifCh:
			if ok {
				s.notifyChan <- ev
			} else {
				connLost = true
			}
		case ev, ok := <-blCh:
			if ok {
				s.blockChan <- ev
			} else {
				connLost = true
			}
		case ev, ok := <-notaryCh:
			if ok {
				s.notaryChan <- ev
			} else {
				connLost = true
			}
		case _, ok := <-cliCh:
			connLost = !ok
		case ok := <-restoreCh:
			restoreInProgress = false
			if !ok {
				connLost = true
			}
		}
		if connLost {
			if !restoreInProgress {
				s.log.Info("RPC connection lost, attempting reconnect")
				if !s.client.SwitchRPC() {
					s.log.Error("can't switch RPC node")
					break routeloop
				}
				cliCh = s.client.NotificationChannel()

				s.Lock()
				s.curNotifyChan = make(chan *state.ContainedNotificationEvent)
				s.curBlockChan = make(chan *block.Block)
				s.curNotaryChan = make(chan *result.NotaryRequestEvent)
				go s.restoreSubscriptions(s.curNotifyChan, s.curBlockChan, s.curNotaryChan, restoreCh)
				s.Unlock()
				restoreInProgress = true
			drainloop:
				for {
					select {
					case _, ok := <-notifCh:
						if !ok {
							notifCh = nil
						}
					case _, ok := <-blCh:
						if !ok {
							blCh = nil
						}
					case _, ok := <-notaryCh:
						if !ok {
							notaryCh = nil
						}
					default:
						break drainloop
					}
				}
			} else { // Avoid getting additional !ok events.
				s.Lock()
				s.curNotifyChan = nil
				s.curBlockChan = nil
				s.curNotaryChan = nil
				s.Unlock()
			}
		}
	}
	close(s.notifyChan)
	close(s.blockChan)
	close(s.notaryChan)
}

// restoreSubscriptions restores subscriptions according to
// cached information about them.
func (s *subscriber) restoreSubscriptions(notifCh chan<- *state.ContainedNotificationEvent,
	blCh chan<- *block.Block, notaryCh chan<- *result.NotaryRequestEvent, resCh chan<- bool) {
	var err error

	// new block events restoration
	if s.subscribedToNewBlocks {
		_, err = s.client.ReceiveBlocks(blCh)
		if err != nil {
			s.log.Error("could not restore block subscription",
				zap.Error(err),
			)
			resCh <- false
			return
		}
	}

	// notification events restoration
	for contract := range s.subscribedEvents {
		contract := contract // See https://github.com/nspcc-dev/neo-go/issues/2890
		_, err = s.client.ReceiveExecutionNotifications(contract, notifCh)
		if err != nil {
			s.log.Error("could not restore notification subscription after RPC switch",
				zap.Error(err),
			)
			resCh <- false
			return
		}
	}

	// notary notification events restoration
	for signer := range s.subscribedNotaryEvents {
		signer := signer // See https://github.com/nspcc-dev/neo-go/issues/2890
		_, err = s.client.ReceiveNotaryRequests(signer, notaryCh)
		if err != nil {
			s.log.Error("could not restore notary notification subscription after RPC switch",
				zap.Error(err),
			)
			resCh <- false
			return
		}
	}
	resCh <- true
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
