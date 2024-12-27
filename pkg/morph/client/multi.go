package client

import (
	"time"

	"go.uber.org/zap"
)

// SwitchRPC performs reconnection and returns new if it was successful.
func (c *Client) switchRPC() *connection {
	var conn = c.conn.Swap(nil)

	if conn != nil {
		conn.Close() // Ensure it's completed and drained.
	}
	for {
		conn = c.connEndpoints()
		if conn != nil {
			c.conn.Store(conn)
			if c.cfg.rpcSwitchCb != nil {
				c.cfg.rpcSwitchCb()
			}

			return conn
		}

		select {
		case <-time.After(c.cfg.reconnectionDelay):
		case <-c.closeChan:
			return nil
		}
	}
}

func (c *Client) connEndpoints() *connection {
	c.cfg.endpointsLock.RLock()
	defer c.cfg.endpointsLock.RUnlock()

	// Iterate endpoints.
	for _, e := range c.cfg.endpoints {
		conn, err := c.newConnection(e)
		if err != nil {
			c.logger.Warn("could not establish connection to RPC node",
				zap.String("endpoint", e),
				zap.Error(err),
			)

			continue
		}

		c.cache.invalidate()

		c.logger.Info("connection to RPC node has been established",
			zap.String("endpoint", e))

		return conn
	}
	return nil
}

func (c *Client) closeWaiter() {
	select {
	case <-c.cfg.ctx.Done():
	case <-c.closeChan:
	}
	var conn = c.conn.Swap(nil)
	if conn != nil {
		conn.Close()
	}
}
