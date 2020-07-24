package network

import (
	"context"
	"net"
	"time"

	manet "github.com/multiformats/go-multiaddr-net"
)

// Dial connects to a remote node by address.
func Dial(ctx context.Context, addr Address) (net.Conn, error) {
	return dialContext(ctx, addr, 0)
}

// DialWithTimeout connects to a remote node by address with timeout.
func DialWithTimeout(ctx context.Context, addr Address, timeout time.Duration) (net.Conn, error) {
	return dialContext(ctx, addr, timeout)
}

func dialContext(ctx context.Context, addr Address, timeout time.Duration) (net.Conn, error) {
	dialer := manet.Dialer{}
	dialer.Timeout = timeout

	return dialer.DialContext(ctx, addr)
}
