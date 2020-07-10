package transport

import (
	"sync/atomic"

	manet "github.com/multiformats/go-multiaddr-net"
)

type (
	// Connection is an interface of network connection.
	Connection interface {
		manet.Conn
		Closed() bool
	}

	conn struct {
		manet.Conn
		closed *int32
	}
)

func newConnection(con manet.Conn) Connection {
	return &conn{
		Conn:   con,
		closed: new(int32),
	}
}

// Closed checks that connection closed.
func (c *conn) Closed() bool { return atomic.LoadInt32(c.closed) == 1 }

// Close connection and write state.
func (c *conn) Close() error {
	if atomic.CompareAndSwapInt32(c.closed, 0, 1) {
		return c.Conn.Close()
	}

	return nil
}
