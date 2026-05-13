package network

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// AddressGroup represents list of network addresses of the node.
//
// List is sorted by priority of use.
type AddressGroup []Address

// String implements fmt.Stringer for AddressGroup.
// Returns concatenation of all addresses from the AddressGroup.
// The result is order-dependent.
func (x AddressGroup) String() string {
	var s strings.Builder

	for addr := range slices.Values(x) {
		s.WriteString(addr.String())
	}

	return s.String()
}

// Len returns number of addresses in AddressGroup.
func (x AddressGroup) Len() int {
	return len(x)
}

// Less returns true if i-th address in AddressGroup supports TLS
// and j-th one doesn't.
func (x AddressGroup) Less(i, j int) bool {
	return x[i].isTLSEnabled() && !x[j].isTLSEnabled()
}

// Swap swaps i-th and j-th addresses in AddressGroup.
func (x AddressGroup) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

// FromStringSlice forms AddressGroup from a string slice.
//
// Returns an error in the absence of addresses or if any of the addresses are incorrect.
func (x *AddressGroup) FromStringSlice(addr []string) error {
	if len(addr) == 0 {
		return errors.New("missing network addresses")
	}

	res := make(AddressGroup, len(addr))
	for i := range addr {
		var a Address
		if err := a.FromString(addr[i]); err != nil {
			return err // invalid format, ignore the whole field
		}
		res[i] = a
	}

	*x = res
	return nil
}

// FromNodeInfo forms AddressGroup from [netmap.NodeInfo] structure.
// The result is sorted with sort.Sort.
//
// Returns an error in the absence of addresses or if any of the addresses are incorrect.
func (x *AddressGroup) FromNodeInfo(ni netmap.NodeInfo) error {
	as := *x

	addrNum := ni.NumberOfNetworkEndpoints()
	if addrNum <= 0 {
		return errors.New("missing network addresses")
	}

	as = slices.Grow(as, addrNum)[:0]
	err := iterateParsedAddresses(ni, func(a Address) error {
		as = append(as, a)
		return nil
	})

	if err == nil {
		sort.Sort(as)
		*x = as
	}

	return err
}

// iterateParsedAddresses parses each address from MultiAddressIterator and passes it to f
// until 1st parsing failure or f's error.
func iterateParsedAddresses(ni netmap.NodeInfo, f func(s Address) error) error {
	for s := range ni.NetworkEndpoints() {
		var a Address

		err := a.FromString(s)
		if err != nil {
			return fmt.Errorf("could not parse address from string: %w", err)
		}

		if err = f(a); err != nil {
			return err
		}
	}
	return nil
}

// WriteToNodeInfo writes AddressGroup to netmap.NodeInfo structure.
func WriteToNodeInfo(g AddressGroup, ni *netmap.NodeInfo) {
	num := g.Len()
	addrs := make([]string, 0, num)

	for addr := range slices.Values(g) {
		ni.SetNetworkEndpoints()
		addrs = append(addrs, addr.String())
	}

	ni.SetNetworkEndpoints(addrs...)
}

// Intersects checks if two AddressGroup have
// at least one common address.
func (x AddressGroup) Intersects(x2 AddressGroup) bool {
	for i := range x {
		if slices.ContainsFunc(x2, x[i].equal) {
			return true
		}
	}

	return false
}
