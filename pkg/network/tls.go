package network

import (
	"github.com/multiformats/go-multiaddr"
)

const (
	tlsProtocolName = "tls"
)

// tls var is used for (un)wrapping other multiaddrs around TLS multiaddr.
var tls, _ = multiaddr.NewMultiaddr("/" + tlsProtocolName)

// isTLSEnabled searches for wrapped TLS protocol in multiaddr.
func (a Address) isTLSEnabled() bool {
	for _, protoc := range a.ma.Protocols() {
		if protoc.Code == multiaddr.P_TLS {
			return true
		}
	}

	return false
}
