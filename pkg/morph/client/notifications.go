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
	var conn = c.conn.Load()

	if conn == nil {
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
		id, err := conn.client.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &contract}, conn.notifyChan)
		if err != nil {
			// if there is some error, undo all subscriptions and return error
			for _, id := range notifyIDs {
				_ = conn.client.Unsubscribe(id)
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
	var conn = c.conn.Load()

	if conn == nil {
		return ErrConnectionLost
	}

	_, err := conn.client.ReceiveBlocks(nil, conn.blockChan)
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

	var conn = c.conn.Load()

	if conn == nil {
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

	_, err := conn.client.ReceiveNotaryRequests(filter, conn.notaryChan)
	if err != nil {
		return fmt.Errorf("subscribe to notary requests RPC: %w", err)
	}

	c.subs.subscribedNotaryEvents[txSigner] = struct{}{}

	return nil
}

// ReceiveAllNotaryRequests subscribes to all [mempoolevent.TransactionAdded]
// notary pool events coming from the Neo blockchain the Client is connected to.
// Events are sent to the channel returned from [Client.Notifications].
//
// See also [Client.ReceiveNotaryRequests].
func (c *Client) ReceiveAllNotaryRequests() error {
	var conn = c.conn.Load()

	if conn == nil {
		return ErrConnectionLost
	}

	c.subs.Lock()
	defer c.subs.Unlock()

	if c.subs.subscribedToAllNotaryEvents {
		return nil
	}

	nrAddedType := mempoolevent.TransactionAdded
	filter := &neorpc.NotaryRequestFilter{Type: &nrAddedType}
	_, err := conn.client.ReceiveNotaryRequests(filter, conn.notaryChan)
	if err != nil {
		return fmt.Errorf("subscribe to notary requests RPC: %w", err)
	}

	c.subs.subscribedToAllNotaryEvents = true

	clear(c.subs.subscribedNotaryEvents)

	return nil
}

// UnsubscribeAll removes all active subscriptions of current client.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) UnsubscribeAll() error {
	var conn = c.conn.Load()

	if conn == nil {
		return ErrConnectionLost
	}

	err := conn.client.UnsubscribeAll()
	if err != nil {
		return err
	}

	return nil
}

// Notifications returns channels than receive subscribed
// notification from the connected RPC node.
// Channels are closed when connections to the RPC nodes are lost.
func (c *Client) Notifications() (<-chan *state.ContainedNotificationEvent, <-chan *block.Block, <-chan *result.NotaryRequestEvent) {
	return c.subs.notifyChan, c.subs.blockChan, c.subs.notaryChan
}

type subscriptions struct {
	// notification consumers (Client sends
	// notifications to these channels)
	notifyChan chan *state.ContainedNotificationEvent
	blockChan  chan *block.Block
	notaryChan chan *result.NotaryRequestEvent

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
		restoreCh = make(chan bool)
		conn      = c.conn.Load()
	)

routeloop:
	for {
		if conn == nil {
			c.logger.Info("RPC connection lost, attempting reconnect")
			conn = c.switchRPC()
			if conn == nil {
				break routeloop
			}
			go c.restoreSubscriptions(conn, restoreCh)
		}
		var connLost bool
		select {
		case <-c.closeChan:
			break routeloop
		case ev, ok := <-conn.notifyChan:
			connLost = handleEv(c.subs.notifyChan, ok, ev)
		case ev, ok := <-conn.blockChan:
			connLost = handleEv(c.subs.blockChan, ok, ev)
		case ev, ok := <-conn.notaryChan:
			connLost = handleEv(c.subs.notaryChan, ok, ev)
		case ok := <-restoreCh:
			if !ok {
				connLost = true
			}
		}
		if connLost {
			conn = nil
		}
	}
	close(c.subs.notifyChan)
	close(c.subs.blockChan)
	close(c.subs.notaryChan)
}

// restoreSubscriptions restores subscriptions according to
// cached information about them.
func (c *Client) restoreSubscriptions(conn *connection, resCh chan<- bool) {
	var err error

	c.subs.RLock()
	defer c.subs.RUnlock()
	// new block events restoration
	if c.subs.subscribedToNewBlocks {
		_, err = conn.client.ReceiveBlocks(nil, conn.blockChan)
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
		_, err = conn.client.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &contract}, conn.notifyChan)
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
		nrAddedType := mempoolevent.TransactionAdded
		filter := &neorpc.NotaryRequestFilter{Type: &nrAddedType}
		_, err = conn.client.ReceiveNotaryRequests(filter, conn.notaryChan)
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

		_, err = conn.client.ReceiveNotaryRequests(filter, conn.notaryChan)
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
