package network

import (
	"errors"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

const (
	// maxProtocolsAmount is maximal amount of protocols
	// in multiaddress after parsing with network.AddressFromString.
	maxProtocolsAmount = 3

	// minProtocolsAmount is minimal amount of protocols
	// in multiaddress after parsing with network.AddressFromString:
	// host(ip) and port.
	minProtocolsAmount = 2

	// network protocols.
	dns, ip4, ip6 = "dns4", "ip4", "ip6"

	// transport protocols.
	tcp = "tcp"
)

var (
	errIncorrectProtocolAmount         = errors.New("numbers of protocols in multiaddress incorrect")
	errUnsupportedNetworkProtocol      = errors.New("unsupported network protocol in multiaddress")
	errUnsupportedTransportProtocol    = errors.New("unsupported transport protocol in multiaddress")
	errUnsupportedPresentationProtocol = errors.New("unsupported presentation protocol in multiaddress")
)

// NodeEndpointsIterator is a wrapper over netmap.NodeInfo which implements
// MultiAddressIterator.
type NodeEndpointsIterator netmap.NodeInfo

func (x NodeEndpointsIterator) IterateAddresses(f func(string) bool) {
	(netmap.NodeInfo)(x).IterateNetworkEndpoints(func(s string) bool { return !f(s) })
}

func (x NodeEndpointsIterator) NumberOfAddresses() int {
	return (netmap.NodeInfo)(x).NumberOfNetworkEndpoints()
}

// VerifyMultiAddress validates multiaddress of n.
//
// If n's address contains more than 3 protocols
// or less than 2 protocols an error returns.
//
// If n's address's protocol order is incorrect
// an error returns.
//
// Correct composition(and order from low to high level)
// of protocols:
//  1. dns4/ip4/ip6
//  2. tcp
//  3. tls(optional, may be absent)
func VerifyMultiAddress(ni netmap.NodeInfo) error {
	return iterateParsedAddresses(NodeEndpointsIterator(ni), checkProtocols)
}

func checkProtocols(a Address) error {
	pp := a.ma.Protocols()
	parsedProtocolsAmount := len(pp)

	if parsedProtocolsAmount > maxProtocolsAmount || parsedProtocolsAmount < minProtocolsAmount {
		return errIncorrectProtocolAmount
	}

	switch pp[0].Name {
	case dns, ip4, ip6:
	default:
		return errUnsupportedNetworkProtocol
	}

	if pp[1].Name != tcp {
		return errUnsupportedTransportProtocol
	}

	if parsedProtocolsAmount != minProtocolsAmount {
		if pp[2].Name != tlsProtocolName {
			return errUnsupportedPresentationProtocol
		}
	}

	return nil
}
