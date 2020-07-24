package network

import (
	"net"

	manet "github.com/multiformats/go-multiaddr-net"
)

// Listen announces on the local network address.
func Listen(addr Address) (net.Listener, error) {
	mLis, err := manet.Listen(addr)
	if err != nil {
		return nil, err
	}

	return manet.NetListener(mLis), nil
}
