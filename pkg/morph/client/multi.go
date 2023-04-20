package client

import (
	"sort"
	"time"

	"go.uber.org/zap"
)

// Endpoint represents morph endpoint together with its priority.
type Endpoint struct {
	Address  string
	Priority int
}

type endpoints struct {
	curr int
	list []Endpoint
}

func (e *endpoints) init(ee []Endpoint) {
	sort.SliceStable(ee, func(i, j int) bool {
		return ee[i].Priority < ee[j].Priority
	})

	e.curr = 0
	e.list = ee
}

// SwitchRPC performs reconnection and returns true if it was successful.
func (c *Client) SwitchRPC() bool {
	c.switchLock.Lock()
	defer c.switchLock.Unlock()

	c.client.Close()

	// Iterate endpoints in the order of decreasing priority.
	for c.endpoints.curr = range c.endpoints.list {
		newEndpoint := c.endpoints.list[c.endpoints.curr].Address
		cli, act, err := c.newCli(newEndpoint)
		if err != nil {
			c.logger.Warn("could not establish connection to the switched RPC node",
				zap.String("endpoint", newEndpoint),
				zap.Error(err),
			)

			continue
		}

		c.cache.invalidate()

		c.logger.Info("connection to the new RPC node has been established",
			zap.String("endpoint", newEndpoint))

		c.client = cli
		c.setActor(act)

		if c.cfg.switchInterval != 0 && !c.switchIsActive.Load() &&
			c.endpoints.list[c.endpoints.curr].Priority != c.endpoints.list[0].Priority {
			c.switchIsActive.Store(true)
			go c.switchToMostPrioritized()
		}

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

func (c *Client) switchToMostPrioritized() {
	t := time.NewTicker(c.cfg.switchInterval)
	defer t.Stop()
	defer c.switchIsActive.Store(false)

mainLoop:
	for {
		select {
		case <-c.cfg.ctx.Done():
			return
		case <-t.C:
			c.switchLock.RLock()
			endpointsCopy := make([]Endpoint, len(c.endpoints.list))
			copy(endpointsCopy, c.endpoints.list)

			currPriority := c.endpoints.list[c.endpoints.curr].Priority
			highestPriority := c.endpoints.list[0].Priority
			c.switchLock.RUnlock()

			if currPriority == highestPriority {
				// already connected to
				// the most prioritized
				return
			}

			for i, e := range endpointsCopy {
				if currPriority == e.Priority {
					// a switch will not increase the priority
					continue mainLoop
				}

				tryE := e.Address

				cli, act, err := c.newCli(tryE)
				if err != nil {
					c.logger.Warn("could not create client to the higher priority node",
						zap.String("endpoint", tryE),
						zap.Error(err),
					)
					continue
				}

				c.switchLock.Lock()

				// higher priority node could have been
				// connected in the other goroutine
				if e.Priority >= c.endpoints.list[c.endpoints.curr].Priority {
					cli.Close()
					c.switchLock.Unlock()
					return
				}

				c.client.Close()
				c.cache.invalidate()
				c.client = cli
				c.setActor(act)
				c.endpoints.curr = i

				c.switchLock.Unlock()

				c.logger.Info("switched to the higher priority RPC",
					zap.String("endpoint", tryE))

				return
			}
		}
	}
}

// close closes notification channel and wrapped WS client.
func (c *Client) close() {
	c.client.Close()
}
