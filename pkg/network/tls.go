package network

import (
	"fmt"

	"github.com/multiformats/go-multiaddr"
)

// There is implementation of TLS protocol for
// github.com/multiformats/go-multiaddr library in this file.
//
// After addition TLS protocol via `multiaddr.AddProtocol` function
// the library is ready to parse "tls" protocol with empty body, e.g.:
//
//		"/dns4/localhost/tcp/8080/tls"

const (
	tlsProtocolName = "tls"

	// tlsProtocolCode is chosen according to its draft version's code in
	// original multiaddr repository: https://github.com/multiformats/multicodec.
	tlsProtocolCode = 0x01c0
)

// tls var is used for (un)wrapping other multiaddrs around TLS multiaddr.
var tls multiaddr.Multiaddr

func init() {
	tlsProtocol := multiaddr.Protocol{
		Name:  tlsProtocolName,
		Code:  tlsProtocolCode,
		Size:  0,
		VCode: multiaddr.CodeToVarint(tlsProtocolCode),
	}

	err := multiaddr.AddProtocol(tlsProtocol)
	if err != nil {
		panic(fmt.Errorf("could not add 'TLS' protocol to multiadd library: %w", err))
	}

	tls, err = multiaddr.NewMultiaddr("/" + tlsProtocolName)
	if err != nil {
		panic(fmt.Errorf("could not init 'TLS' protocol with multiadd library: %w", err))
	}
}

// TLSEnabled searches for wrapped TLS protocol in multiaddr.
func (a Address) TLSEnabled() bool {
	for _, protoc := range a.ma.Protocols() {
		if protoc.Code == tlsProtocolCode {
			return true
		}
	}

	return false
}
