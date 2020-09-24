package network

import (
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/pkg/errors"
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

func (a Address) String() string {
	return a.ma.String()
}

// NetAddr returns network endpoint address in string format.
func (a Address) NetAddr() (string, error) {
	ip, err := manet.ToNetAddr(a.ma)
	if err != nil {
		return "", errors.Wrap(err, "could not get net addr")
	}

	return ip.String(), nil
}

// AddressFromString restores address from a string representation.
func AddressFromString(s string) (*Address, error) {
	ma, err := multiaddr.NewMultiaddr(s)
	if err != nil {
		return nil, err
	}

	return &Address{
		ma: ma,
	}, nil
}

// IsLocalAddress returns true if network endpoint from local address
// source is equal to network endpoint of passed address.
func IsLocalAddress(src LocalAddressSource, addr *Address) bool {
	return src.LocalAddress().ma.Equal(addr.ma)
}
