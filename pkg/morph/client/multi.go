package client

import (
	"go.uber.org/zap"
)

// Endpoint represents morph endpoint together with its priority.
type Endpoint struct {
	Address  string
	Priority int
}

// SwitchRPC performs reconnection and returns true if it was successful.
func (c *Client) SwitchRPC() bool {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	c.client.Close()

	// Iterate endpoints in the order of decreasing priority.
	for _, e := range c.endpoints {
		cli, act, err := c.newCli(e)
		if err != nil {
			c.logger.Warn("could not establish connection to the switched RPC node",
				zap.String("endpoint", e),
				zap.Error(err),
			)

			continue
		}

		c.cache.invalidate()

		c.logger.Info("connection to the new RPC node has been established",
			zap.String("endpoint", e))

		c.client = cli
		c.setActor(act)

		return true
	}

	c.inactive = true

	if c.cfg.inactiveModeCb != nil {
		c.cfg.inactiveModeCb()
	}
	return false
}

func (c *Client) closeWaiter() {
	select {
	case <-c.cfg.ctx.Done():
	case <-c.closeChan:
	}
	_ = c.UnsubscribeAll()
	c.close()
}

// close closes notification channel and wrapped WS client.
func (c *Client) close() {
	c.client.Close()
}
