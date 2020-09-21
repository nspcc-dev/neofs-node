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

func (a Address) String() string {
	return a.ma.String()
}

// NetAddr returns network endpoint address in string format.
func (a Address) NetAddr() string {
	ip, err := manet.ToNetAddr(a.ma)
	if err != nil {
		panic(errors.Wrap(err, "could not get net addr"))
	}

	return ip.String()
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
