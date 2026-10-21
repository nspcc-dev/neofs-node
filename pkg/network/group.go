package network

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/nspcc-dev/neofs-node/internal/uriutil"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// AddressGroup represents list of network addresses of the node.
type AddressGroup []string

// String implements fmt.Stringer for AddressGroup.
// Returns concatenation of all addresses from the AddressGroup.
// The result is order-dependent.
func (x AddressGroup) String() string {
	return strings.Join([]string(x), "")
}

// FromStringSlice forms AddressGroup from a string slice.
//
// Returns an error in the absence of addresses or if any of the addresses are incorrect.
func (x *AddressGroup) FromStringSlice(addr []string) error {
	if len(addr) == 0 {
		return errors.New("missing network addresses")
	}

	var as = make([]string, 0, len(addr))

	err := iterateParsedAddresses(slices.Values(addr), func(a string) error {
		as = append(as, a)
		return nil
	})
	if err == nil {
		*x = as
	}

	return err
}

// FromNodeInfo forms AddressGroup from [netmap.NodeInfo] structure.
//
// Returns an error in the absence of addresses or if any of the addresses are incorrect.
func (x *AddressGroup) FromNodeInfo(ni netmap.NodeInfo) error {
	as := *x

	addrNum := ni.NumberOfNetworkEndpoints()
	if addrNum <= 0 {
		return errors.New("missing network addresses")
	}

	as = slices.Grow(as, addrNum)[:0]
	err := iterateParsedAddresses(ni.NetworkEndpoints(), func(a string) error {
		as = append(as, a)
		return nil
	})

	if err == nil {
		*x = as
	}

	return err
}

// iterateParsedAddresses parses each ni address and passes it to f until
// the first parsing failure or f's error.
func iterateParsedAddresses(addrs iter.Seq[string], f func(s string) error) error {
	for s := range addrs {
		_, _, err := uriutil.Parse(s)
		if err != nil {
			return fmt.Errorf("could not parse address from string: %w", err)
		}

		if err = f(s); err != nil {
			return err
		}
	}
	return nil
}

// WriteToNodeInfo writes AddressGroup to netmap.NodeInfo structure.
func WriteToNodeInfo(g AddressGroup, ni *netmap.NodeInfo) {
	ni.SetNetworkEndpoints(slices.Clone([]string(g))...)
}

// Intersects checks if two AddressGroup have
// at least one common address.
func (x AddressGroup) Intersects(x2 AddressGroup) bool {
	for i := range x {
		if slices.Contains([]string(x2), x[i]) {
			return true
		}
	}

	return false
}
