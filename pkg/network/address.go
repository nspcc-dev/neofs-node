package network

import (
	"github.com/multiformats/go-multiaddr"
)

// Address represents the NeoFS node
// network address.
//
// It is a type alias of
// github.com/multiformats/go-multiaddr.Multiaddr.
type Address = multiaddr.Multiaddr
