package client

import (
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
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

// SubscribeForExecutionNotifications adds subscription for notifications
// generated during contract transaction execution to this instance of client.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) SubscribeForExecutionNotifications(contract util.Uint160) error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, subscribed := c.subscribedEvents[contract]
	if subscribed {
		// no need to subscribe one more time
		return nil
	}

	id, err := c.client.SubscribeForExecutionNotifications(&contract, nil)
	if err != nil {
		return err
	}

	c.subscribedEvents[contract] = id

	return nil
}

// SubscribeForNewBlocks adds subscription for new block events to this
// instance of client.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) SubscribeForNewBlocks() error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	if c.subscribedToNewBlocks {
		// no need to subscribe one more time
		return nil
	}

	_, err := c.client.SubscribeForNewBlocks(nil)
	if err != nil {
		return err
	}

	c.subscribedToNewBlocks = true

	return nil
}

// SubscribeForNotaryRequests adds subscription for notary request payloads
// addition or removal events to this instance of client. Passed txSigner is
// used as filter: subscription is only for the notary requests that must be
// signed by txSigner.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) SubscribeForNotaryRequests(txSigner util.Uint160) error {
	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, subscribed := c.subscribedNotaryEvents[txSigner]
	if subscribed {
		// no need to subscribe one more time
		return nil
	}

	id, err := c.client.SubscribeForNotaryRequests(nil, &txSigner)
	if err != nil {
		return err
	}

	c.subscribedNotaryEvents[txSigner] = id

	return nil
}

// UnsubscribeContract removes subscription for given contract event stream.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) UnsubscribeContract(contract util.Uint160) error {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, subscribed := c.subscribedEvents[contract]
	if !subscribed {
		// no need to unsubscribe contract
		// without subscription
		return nil
	}

	err := c.client.Unsubscribe(c.subscribedEvents[contract])
	if err != nil {
		return err
	}

	delete(c.subscribedEvents, contract)

	return nil
}

// UnsubscribeNotaryRequest removes subscription for given notary requests
// signer.
//
// Returns ErrConnectionLost if client has not been able to establish
// connection to any of passed RPC endpoints.
func (c *Client) UnsubscribeNotaryRequest(signer util.Uint160) error {
	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	if c.inactive {
		return ErrConnectionLost
	}

	_, subscribed := c.subscribedNotaryEvents[signer]
	if !subscribed {
		// no need to unsubscribe signer's
		// requests without subscription
		return nil
	}

	err := c.client.Unsubscribe(c.subscribedNotaryEvents[signer])
	if err != nil {
		return err
	}

	delete(c.subscribedNotaryEvents, signer)

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

	// no need to unsubscribe if there are
	// no active subscriptions
	if len(c.subscribedEvents) == 0 && len(c.subscribedNotaryEvents) == 0 &&
		!c.subscribedToNewBlocks {
		return nil
	}

	err := c.client.UnsubscribeAll()
	if err != nil {
		return err
	}

	c.subscribedEvents = make(map[util.Uint160]string)
	c.subscribedNotaryEvents = make(map[util.Uint160]string)
	c.subscribedToNewBlocks = false

	return nil
}

// restoreSubscriptions restores subscriptions according to
// cached information about them.
func (c *Client) restoreSubscriptions(cli *rpcclient.WSClient, endpoint string) bool {
	var (
		err error
		id  string
	)

	// new block events restoration
	if c.subscribedToNewBlocks {
		_, err = cli.SubscribeForNewBlocks(nil)
		if err != nil {
			c.logger.Error("could not restore block subscription after RPC switch",
				zap.String("endpoint", endpoint),
				zap.Error(err),
			)

			return false
		}
	}

	// notification events restoration
	for contract := range c.subscribedEvents {
		contract := contract // See https://github.com/nspcc-dev/neo-go/issues/2890
		id, err = cli.SubscribeForExecutionNotifications(&contract, nil)
		if err != nil {
			c.logger.Error("could not restore notification subscription after RPC switch",
				zap.String("endpoint", endpoint),
				zap.Error(err),
			)

			return false
		}

		c.subscribedEvents[contract] = id
	}

	// notary notification events restoration
	if c.notary != nil {
		for signer := range c.subscribedNotaryEvents {
			signer := signer // See https://github.com/nspcc-dev/neo-go/issues/2890
			id, err = cli.SubscribeForNotaryRequests(nil, &signer)
			if err != nil {
				c.logger.Error("could not restore notary notification subscription after RPC switch",
					zap.String("endpoint", endpoint),
					zap.Error(err),
				)

				return false
			}

			c.subscribedNotaryEvents[signer] = id
		}
	}

	return true
}
