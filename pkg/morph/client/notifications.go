package client

import (
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// Close closes connection to the remote side making
// this client instance unusable. Closes notification
// channel returned from Client.NotificationChannel(),
// Removes all subscription.
func (c *Client) Close() {
	// closing should be done via the channel
	// to prevent switching to another RPC node
	// in the notification loop
	c.closeChan <- struct{}{}
}

// ReceiveExecutionNotifications performs subscription for notifications
// generated during contract execution. Events are sent to the specified channel.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) ReceiveExecutionNotifications(contract util.Uint160, ch chan<- *state.ContainedNotificationEvent) (string, error) {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return "", ErrConnectionLost
	}

	return c.client.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &contract}, ch)
}

// ReceiveBlocks performs subscription for new block events. Events are sent
// to the specified channel.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) ReceiveBlocks(ch chan<- *block.Block) (string, error) {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return "", ErrConnectionLost
	}

	return c.client.ReceiveBlocks(nil, ch)
}

// ReceiveNotaryRequests performsn subscription for notary request payloads
// addition or removal events to this instance of client. Passed txSigner is
// used as filter: subscription is only for the notary requests that must be
// signed by txSigner. Events are sent to the specified channel.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) ReceiveNotaryRequests(txSigner util.Uint160, ch chan<- *result.NotaryRequestEvent) (string, error) {
	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return "", ErrConnectionLost
	}

	return c.client.ReceiveNotaryRequests(&neorpc.TxFilter{Signer: &txSigner}, ch)
}

// Unsubscribe performs unsubscription for the given subscription ID.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) Unsubscribe(subID string) error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	return c.client.Unsubscribe(subID)
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
