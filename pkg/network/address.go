package network

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

/*
	HostAddr strings: 	"localhost:8080", ":8080", "192.168.0.1:8080"
	MultiAddr strings: 	"/dns4/localhost/tcp/8080", "/ip4/192.168.0.1/tcp/8080"
	IPAddr strings:		"192.168.0.1:8080"
	URIAddr strings:	"<scheme://>127.0.0.1:8080"
*/

// Address represents the NeoFS node
// network address.
type Address struct {
	ma multiaddr.Multiaddr
}

// LocalAddressSource is an interface of local
// network address container with read access.
type LocalAddressSource interface {
	LocalAddress() AddressGroup
}

// String returns multiaddr string.
func (a Address) String() string {
	return a.ma.String()
}

// equal compares Address's.
func (a Address) equal(addr Address) bool {
	return a.ma.Equal(addr.ma)
}

// HostAddr returns host address in string format.
//
// Panics if host address cannot be fetched from Address.
func (a Address) HostAddr() string {
	_, host, err := manet.DialArgs(a.ma)
	if err != nil {
		// the only correct way to construct Address is AddressFromString
		// which makes this error appear unexpected
		panic(fmt.Errorf("could not get host addr: %w", err))
	}

	return host
}

// FromString restores Address from a string representation.
//
// Supports URIAddr, MultiAddr and HostAddr strings.
func (a *Address) FromString(s string) error {
	var err error

	a.ma, err = multiaddr.NewMultiaddr(s)
	if err != nil {
		var u uri

		u.parse(s)

		s, err = multiaddrStringFromHostAddr(u.host)
		if err == nil {
			a.ma, err = multiaddr.NewMultiaddr(s)
			if err == nil && u.tls {
				a.ma = a.ma.Encapsulate(tls)
			}
		}
	}

	return err
}

type uri struct {
	host string
	tls  bool
}

const grpcTLSScheme = "grpcs"

func (u *uri) parse(s string) {
	// TODO: code is copy-pasted from client.WithURIAddress function.
	//  Would be nice to share the code.
	uri, err := url.ParseRequestURI(s)
	isURI := err == nil

	if isURI && uri.Host != "" {
		u.host = uri.Host
	} else {
		u.host = s
	}

	// check if passed string was parsed correctly
	// URIs that do not start with a slash after the scheme are interpreted as:
	// `scheme:opaque` => if `opaque` is not empty, then it is supposed that URI
	// is in `host:port` format
	u.tls = isURI && uri.Opaque == "" && uri.Scheme == grpcTLSScheme
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

// IsLocalAddress returns true if network endpoints from local address group
// source intersects with network endpoints of passed address group.
func IsLocalAddress(src LocalAddressSource, addr AddressGroup) bool {
	return src.LocalAddress().Intersects(addr)
}
