package client

import (
	"go.uber.org/zap"
)

type endpoints struct {
	curr int
	list []string
}

func newEndpoints(ee []string) *endpoints {
	return &endpoints{
		curr: 0,
		list: ee,
	}
}

// next returns the next endpoint and its index
// to try to connect to.
// Returns -1 index if there is no known RPC endpoints.
func (e *endpoints) next() (string, int) {
	if len(e.list) == 0 {
		return "", -1
	}

	next := e.curr + 1
	if next == len(e.list) {
		next = 0
	}

	e.curr = next

	return e.list[next], next
}

// current returns an endpoint and its index the Client
// is connected to.
// Returns -1 index if there is no known RPC endpoints
func (e *endpoints) current() (string, int) {
	if len(e.list) == 0 {
		return "", -1
	}

	return e.list[e.curr], e.curr
}

func (c *Client) switchRPC() bool {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	c.client.Close()

	_, currEndpointIndex := c.endpoints.current()
	if currEndpointIndex == -1 {
		// there are no known RPC endpoints to try
		// to connect to => do not switch
		return false
	}

	for {
		newEndpoint, index := c.endpoints.next()
		if index == currEndpointIndex {
			// all the endpoint have been tried
			// for connection unsuccessfully
			return false
		}

		cli, err := newWSClient(c.cfg, newEndpoint)
		if err != nil {
			c.logger.Warn("could not establish connection to the switched RPC node",
				zap.String("endpoint", newEndpoint),
				zap.Error(err),
			)

			continue
		}

		err = cli.Init()
		if err != nil {
			cli.Close()
			c.logger.Warn("could not init the switched RPC node",
				zap.String("endpoint", newEndpoint),
				zap.Error(err),
			)

			continue
		}

		c.cache.invalidate()
		c.client = cli

		return true
	}
}

func (c *Client) notificationLoop() {
	for {
		select {
		case <-c.cfg.ctx.Done():
			_ = c.UnsubscribeAll()
			c.close()

			return
		case <-c.closeChan:
			_ = c.UnsubscribeAll()
			c.close()

			return
		case n, ok := <-c.client.Notifications:
			// notification channel is used as a connection
			// state: if it is closed, the connection is
			// considered to be lost
			if !ok {
				var closeReason string
				if closeErr := c.client.GetError(); closeErr != nil {
					closeReason = closeErr.Error()
				} else {
					closeReason = "unknown"
				}

				c.logger.Warn("switching to the next RPC node",
					zap.String("reason", closeReason),
				)

				if !c.switchRPC() {
					c.logger.Error("could not establish connection to any RPC node")

					// could not connect to all endpoints =>
					// switch client to inactive mode
					c.inactiveMode()

					return
				}

				newEndpoint, _ := c.endpoints.current()

				c.logger.Warn("connection to the new RPC node has been established",
					zap.String("endpoint", newEndpoint),
				)

				if !c.restoreSubscriptions() {
					// new WS client does not allow
					// restoring subscription, client
					// could not work correctly =>
					// closing connection to RPC node
					// to switch to another one
					c.client.Close()
				}

				// TODO(@carpawell): call here some callback retrieved in constructor
				// of the client to allow checking chain state since during switch
				// process some notification could be lost

				continue
			}

			c.notifications <- n
		}
	}
}

// close closes notification channel and wrapped WS client
func (c *Client) close() {
	close(c.notifications)
	c.client.Close()
}
