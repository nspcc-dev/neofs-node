package client

import (
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

// Close closes connection to the remote side making
// this client instance unusable. Closes notification
// channel returned from Client.Notifications(),
// Removes all subscription.
func (c *Client) Close() {
	// closing should be done via the channel
	// to prevent switching to another RPC node
	// in the notification loop
	close(c.closeChan)
}

// ReceiveExecutionNotifications performs subscription for notifications
// generated during contract execution. Notification channel may be
// acquired with [Notifications] method.
// The channel is closed when connection to RPC nodes is lost.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) ReceiveExecutionNotifications(contracts []util.Uint160) error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	notifyIDs := make([]string, 0, len(contracts))
	for _, contract := range contracts {
		c.subs.RLock()
		_, ok := c.subs.subscribedEvents[contract]
		c.subs.RUnlock()

		if ok {
			continue
		}

		// subscribe to contract notifications
		id, err := c.client.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &contract}, c.subs.curNotifyChan)
		if err != nil {
			// if there is some error, undo all subscriptions and return error
			for _, id := range notifyIDs {
				_ = c.client.Unsubscribe(id)
			}

			return fmt.Errorf("contract events subscription RPC: %w", err)
		}

		// save notification id
		notifyIDs = append(notifyIDs, id)
	}

	c.subs.Lock()
	for i := range contracts {
		c.subs.subscribedEvents[contracts[i]] = struct{}{}
	}
	c.subs.Unlock()

	return nil
}

// ReceiveBlocks performs subscription for new block events. Events are sent
// to a returned channel.
// The channel is closed when connection to RPC nodes is lost.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) ReceiveBlocks() error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, err := c.client.ReceiveBlocks(nil, c.subs.curBlockChan)
	if err != nil {
		return fmt.Errorf("block subscriptions RPC: %w", err)
	}

	c.subs.Lock()
	c.subs.subscribedToNewBlocks = true
	c.subs.Unlock()

	return nil
}

// ReceiveNotaryRequests performs subscription for notary request payloads
// addition or removal events to this instance of client. Passed txSigner
// expands the flow of notary requests to those whose main transaction signers
// include the specified account. Events are sent to a returned channel. The
// channel is closed when connection to RPC nodes is lost.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
//
// See also [Client.ReceiveAllNotaryRequests].
func (c *Client) ReceiveNotaryRequests(txSigner util.Uint160) error {
	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	c.subs.Lock()
	defer c.subs.Unlock()

	if c.subs.subscribedToAllNotaryEvents {
		return nil
	}

	if _, ok := c.subs.subscribedNotaryEvents[txSigner]; ok {
		return nil
	}

	nrAddedType := mempoolevent.TransactionAdded
	filter := &neorpc.NotaryRequestFilter{Signer: &txSigner, Type: &nrAddedType}

	_, err := c.client.ReceiveNotaryRequests(filter, c.subs.curNotaryChan)
	if err != nil {
		return fmt.Errorf("subscribe to notary requests RPC: %w", err)
	}

	c.subs.subscribedNotaryEvents[txSigner] = struct{}{}

	return nil
}

// ReceiveAllNotaryRequests subscribes to all notary request events coming from
// the Neo blockchain the Client connected to. Events are sent to the channel
// returned from [Client.Notifications].
//
// See also [Client.ReceiveNotaryRequests].
func (c *Client) ReceiveAllNotaryRequests() error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	c.subs.Lock()
	defer c.subs.Unlock()

	if c.subs.subscribedToAllNotaryEvents {
		return nil
	}

	_, err := c.client.ReceiveNotaryRequests(nil, c.subs.curNotaryChan)
	if err != nil {
		return fmt.Errorf("subscribe to notary requests RPC: %w", err)
	}

	c.subs.subscribedToAllNotaryEvents = true

	for k := range c.subs.subscribedNotaryEvents {
		delete(c.subs.subscribedNotaryEvents, k)
	}

	return nil
}

// UnsubscribeAll removes all active subscriptions of current client.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) UnsubscribeAll() error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	err := c.client.UnsubscribeAll()
	if err != nil {
		return err
	}

	return nil
}

// Notifications returns channels than receive subscribed
// notification from the connected RPC node.
// Channels are closed when connections to the RPC nodes are lost.
func (c *Client) Notifications() (<-chan *state.ContainedNotificationEvent, <-chan *block.Block, <-chan *result.NotaryRequestEvent) {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	return c.subs.notifyChan, c.subs.blockChan, c.subs.notaryChan
}

type subscriptions struct {
	// notification consumers (Client sends
	// notifications to these channels)
	notifyChan chan *state.ContainedNotificationEvent
	blockChan  chan *block.Block
	notaryChan chan *result.NotaryRequestEvent

	// notification receivers (Client reads
	// notifications from these channels)
	curNotifyChan chan *state.ContainedNotificationEvent
	curBlockChan  chan *block.Block
	curNotaryChan chan *result.NotaryRequestEvent

	sync.RWMutex // for subscription fields only

	// cached subscription information
	subscribedEvents            map[util.Uint160]struct{}
	subscribedToAllNotaryEvents bool
	// particular transaction signers to listen when subscribedToAllNotaryEvents is unset
	subscribedNotaryEvents map[util.Uint160]struct{}
	subscribedToNewBlocks  bool
}

func (c *Client) routeNotifications() {
	var (
		restoreCh         = make(chan bool)
		restoreInProgress bool
	)

routeloop:
	for {
		var connLost bool
		c.switchLock.RLock()
		notifCh := c.subs.curNotifyChan
		blCh := c.subs.curBlockChan
		notaryCh := c.subs.curNotaryChan
		c.switchLock.RUnlock()
		select {
		case <-c.closeChan:
			break routeloop
		case ev, ok := <-notifCh:
			connLost = handleEv(c.subs.notifyChan, ok, ev)
		case ev, ok := <-blCh:
			connLost = handleEv(c.subs.blockChan, ok, ev)
		case ev, ok := <-notaryCh:
			connLost = handleEv(c.subs.notaryChan, ok, ev)
		case ok := <-restoreCh:
			restoreInProgress = false
			if !ok {
				connLost = true
			}
		}
		if connLost {
			if !restoreInProgress {
				c.logger.Info("RPC connection lost, attempting reconnect")
				if !c.SwitchRPC() {
					c.logger.Error("can't switch RPC node")
					break routeloop
				}

				c.subs.Lock()
				c.subs.curNotifyChan = make(chan *state.ContainedNotificationEvent)
				c.subs.curBlockChan = make(chan *block.Block)
				c.subs.curNotaryChan = make(chan *result.NotaryRequestEvent)
				go c.restoreSubscriptions(c.subs.curNotifyChan, c.subs.curBlockChan, c.subs.curNotaryChan, restoreCh)
				c.subs.Unlock()
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
			} else { // Avoid getting additional !ok eventc.subs.
				c.subs.Lock()
				c.subs.curNotifyChan = nil
				c.subs.curBlockChan = nil
				c.subs.curNotaryChan = nil
				c.subs.Unlock()
			}
		}
	}
	close(c.subs.notifyChan)
	close(c.subs.blockChan)
	close(c.subs.notaryChan)
}

// restoreSubscriptions restores subscriptions according to
// cached information about them.
func (c *Client) restoreSubscriptions(notifCh chan<- *state.ContainedNotificationEvent,
	blCh chan<- *block.Block, notaryCh chan<- *result.NotaryRequestEvent, resCh chan<- bool) {
	var err error

	// new block events restoration
	if c.subs.subscribedToNewBlocks {
		_, err = c.client.ReceiveBlocks(nil, blCh)
		if err != nil {
			c.logger.Error("could not restore block subscription",
				zap.Error(err),
			)
			resCh <- false
			return
		}
	}

	// notification events restoration
	for contract := range c.subs.subscribedEvents {
		_, err = c.client.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &contract}, notifCh)
		if err != nil {
			c.logger.Error("could not restore notification subscription after RPC switch",
				zap.Error(err),
			)
			resCh <- false
			return
		}
	}

	// notary notification events restoration
	if c.subs.subscribedToAllNotaryEvents {
		_, err = c.client.ReceiveNotaryRequests(nil, notaryCh)
		if err != nil {
			c.logger.Error("could not restore notary notification subscription after RPC switch",
				zap.Error(err))
		}
		resCh <- err == nil
		return
	}

	for signer := range c.subs.subscribedNotaryEvents {
		nrAddedType := mempoolevent.TransactionAdded
		filter := &neorpc.NotaryRequestFilter{Signer: &signer, Type: &nrAddedType}

		_, err = c.client.ReceiveNotaryRequests(filter, notaryCh)
		if err != nil {
			c.logger.Error("could not restore notary notification subscription after RPC switch",
				zap.Error(err),
			)
			resCh <- false
			return
		}
	}
	resCh <- true
}

func handleEv[T any](ch chan<- T, ok bool, ev T) bool {
	if !ok {
		return true
	}

	ch <- ev

	return false
}
