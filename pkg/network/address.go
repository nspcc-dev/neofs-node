package network

import (
	"fmt"
	"net"
	"strings"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

/*
	HostAddr strings: 	"localhost:8080", ":8080", "192.168.0.1:8080"
	MultiAddr strings: 	"/dns4/localhost/tcp/8080", "/ip4/192.168.0.1/tcp/8080"
	IPAddr strings:		"192.168.0.1:8080"
*/

// Address represents the NeoFS node
// network address.
type Address struct {
	ma multiaddr.Multiaddr
}

// LocalAddressSource is an interface of local
// network address container with read access.
type LocalAddressSource interface {
	LocalAddress() *Address
}

// Encapsulate wraps this Address around another. For example:
//
//		/ip4/1.2.3.4 encapsulate /tcp/80 = /ip4/1.2.3.4/tcp/80
//
func (a *Address) Encapsulate(addr *Address) {
	a.ma = a.ma.Encapsulate(addr.ma)
}

// Decapsulate removes an Address wrapping. For example:
//
//		/ip4/1.2.3.4/tcp/80 decapsulate /ip4/1.2.3.4 = /tcp/80
//
func (a *Address) Decapsulate(addr *Address) {
	a.ma = a.ma.Decapsulate(addr.ma)
}

// String returns multiaddr string
func (a Address) String() string {
	return a.ma.String()
}

// Equal compares Address's.
func (a Address) Equal(addr *Address) bool {
	return a.ma.Equal(addr.ma)
}

// IPAddrString returns network endpoint address in string format.
func (a Address) IPAddrString() (string, error) {
	ip, err := manet.ToNetAddr(a.ma)
	if err != nil {
		return "", fmt.Errorf("could not get net addr: %w", err)
	}

	return ip.String(), nil
}

// HostAddrString returns host address in string format.
func (a Address) HostAddrString() (string, error) {
	_, host, err := manet.DialArgs(a.ma)
	if err != nil {
		return "", fmt.Errorf("could not get host addr: %w", err)
	}

	return host, nil
}

// AddressFromString restores address from a multiaddr string representation.
func AddressFromString(s string) (*Address, error) {
	ma, err := multiaddr.NewMultiaddr(s)
	if err != nil {
		s, err = multiaddrStringFromHostAddr(s)
		if err != nil {
			return nil, err
		}

		ma, err = multiaddr.NewMultiaddr(s) // don't want recursion there
		if err != nil {
			return nil, err
		}
	}

	return &Address{
		ma: ma,
	}, nil
}

// multiaddrStringFromHostAddr converts "localhost:8080" to "/dns4/localhost/tcp/8080"
func multiaddrStringFromHostAddr(host string) (string, error) {
	endpoint, port, err := net.SplitHostPort(host)
	if err != nil {
		return "", err
	}

	// Empty address in host `:8080` generates `/dns4//tcp/8080` multiaddr
	// which is invalid. It could be `/tcp/8080` but this breaks
	// `manet.DialArgs`. The solution is to manually parse it as 0.0.0.0
	if endpoint == "" {
		return "/ip4/0.0.0.0/tcp/" + port, nil
	}

	var (
		prefix = "/dns4"
		addr   = endpoint
	)

	if ip := net.ParseIP(endpoint); ip != nil {
		addr = ip.String()
		if ip.To4() == nil {
			prefix = "/ip6"
		} else {
			prefix = "/ip4"
		}
	}

	const l4Protocol = "tcp"

	return strings.Join([]string{prefix, addr, l4Protocol, port}, "/"), nil
}

// IsLocalAddress returns true if network endpoint from local address
// source is equal to network endpoint of passed address.
func IsLocalAddress(src LocalAddressSource, addr *Address) bool {
	return src.LocalAddress().ma.Equal(addr.ma)
}

// HostAddrFromMultiaddr converts "/dns4/localhost/tcp/8080" to "localhost:8080".
func HostAddrFromMultiaddr(multiaddr string) (string, error) {
	address, err := AddressFromString(multiaddr)
	if err != nil {
		return "", err
	}

	return address.HostAddrString()
}
