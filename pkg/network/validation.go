package network

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// VerifyAddresses validates public addresses of ni.
func VerifyAddresses(ni netmap.NodeInfo) error {
	return iterateParsedAddresses(ni.NetworkEndpoints(), func(_ string) error { return nil })
}
