package muxer

import (
	"net"

	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/pkg/errors"
)

type netListenerAdapter struct {
	manet.Listener
}

var errNothingAccept = errors.New("nothing to accept")

// Accept waits for and returns the next connection to the listener.
func (l *netListenerAdapter) Accept() (net.Conn, error) {
	if l.Listener == nil {
		return nil, errNothingAccept
	}

	return l.Listener.Accept()
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (l *netListenerAdapter) Close() error {
	if l.Listener == nil {
		return nil
	}

	return l.Listener.Close()
}

// Addr returns the net.Listener's network address.
func (l *netListenerAdapter) Addr() net.Addr {
	if l.Listener == nil {
		return (*net.TCPAddr)(nil)
	}

	return l.Listener.Addr()
}

// NetListener turns this Listener into a net.Listener.
//
// * Connections returned from Accept implement multiaddr-net Conn.
// * Calling WrapNetListener on the net.Listener returned by this function will
//   return the original (underlying) multiaddr-net Listener.
func NetListener(l manet.Listener) net.Listener {
	return &netListenerAdapter{Listener: l}
}
