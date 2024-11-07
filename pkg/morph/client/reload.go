package client

import "slices"

// Reload allows runtime reconfiguration for WithEndpoints parameter.
func (c *Client) Reload(opts ...Option) {
	cfg := new(cfg)
	for _, o := range opts {
		o(cfg)
	}

	c.cfg.endpointsLock.Lock()

	c.cfg.endpoints = cfg.endpoints

	c.cfg.endpointsLock.Unlock()

	conn := c.conn.Load()
	if conn == nil {
		return
	}

	// Close current connection and attempt to reconnect, if there is no endpoint
	// in the config to which the client is connected.
	// Node service can be interrupted in this case.
	if slices.Contains(cfg.endpoints, conn.client.Endpoint()) {
		conn.client.Close()
	}
}
