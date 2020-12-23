package network

import (
	"net"
	"strings"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/pkg/errors"
)

/*
	HostAddr strings: 	"localhost:8080", ":8080", "192.168.0.1:8080"
	MultiAddr strings: 	"/dns4/localhost/tcp/8080", "/ip4/192.168.0.1/tcp/8080"
	IPAddr strings:		"192.168.0.1:8080"
*/

const (
	L4Protocol = "tcp"
)

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

// String returns multiaddr string
func (a Address) String() string {
	return a.ma.String()
}

// IPAddrString returns network endpoint address in string format.
func (a Address) IPAddrString() (string, error) {
	ip, err := manet.ToNetAddr(a.ma)
	if err != nil {
		return "", errors.Wrap(err, "could not get net addr")
	}

	return ip.String(), nil
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

	return strings.Join([]string{prefix, addr, L4Protocol, port}, "/"), nil
}

// IsLocalAddress returns true if network endpoint from local address
// source is equal to network endpoint of passed address.
func IsLocalAddress(src LocalAddressSource, addr *Address) bool {
	return src.LocalAddress().ma.Equal(addr.ma)
}

// IPAddrFromMultiaddr converts "/dns4/localhost/tcp/8080" to "192.168.0.1:8080".
func IPAddrFromMultiaddr(multiaddr string) (string, error) {
	address, err := AddressFromString(multiaddr)
	if err != nil {
		return "", err
	}

	return address.IPAddrString()
}
