package network

import (
	"github.com/multiformats/go-multiaddr"
)

// Address represents the NeoFS node
// network address.
type Address struct {
	ma multiaddr.Multiaddr
}

func (a Address) String() string {
	return a.ma.String()
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
