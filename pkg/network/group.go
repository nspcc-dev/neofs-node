package network

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// AddressGroup represents list of network addresses of the node.
//
// List is sorted by priority of use.
type AddressGroup []Address

// StringifyGroup returns concatenation of all addresses
// from the AddressGroup.
//
// The result is order-dependent.
func StringifyGroup(x AddressGroup) string {
	var s string

	for addr := range slices.Values(x) {
		s += addr.String()
	}

	return s
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

// MultiAddressIterator is an interface of network address group.
type MultiAddressIterator interface {
	// IterateAddresses must iterate over network addresses and pass each one
	// to the handler until it returns false.
	IterateAddresses(func(string) bool)

	// NumberOfAddresses must return number of addresses in group.
	NumberOfAddresses() int
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

// FromIterator forms AddressGroup from MultiAddressIterator structure.
// The result is sorted with sort.Sort.
//
// Returns an error in the absence of addresses or if any of the addresses are incorrect.
func (x *AddressGroup) FromIterator(iter MultiAddressIterator) error {
	as := *x

	addrNum := iter.NumberOfAddresses()
	if addrNum <= 0 {
		return errors.New("missing network addresses")
	}

	as = slices.Grow(as, addrNum)[:0]
	err := iterateParsedAddresses(iter, func(a Address) error {
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
func iterateParsedAddresses(iter MultiAddressIterator, f func(s Address) error) error {
	for s := range iter.IterateAddresses {
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
		for j := range x2 {
			if x[i].equal(x2[j]) {
				return true
			}
		}
	}

	return false
}
