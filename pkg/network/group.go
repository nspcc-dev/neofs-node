package network

import (
	"errors"
	"fmt"
	"sort"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
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

	iterateAllAddresses(x, func(addr Address) {
		s += addr.String()
	})

	return s
}

// IterateAddresses iterates over all network addresses of the node.
//
// Breaks iterating on handler's true return.
//
// Handler should not be nil.
func (x AddressGroup) IterateAddresses(f func(Address) bool) {
	for i := range x {
		if f(x[i]) {
			break
		}
	}
}

// iterateAllAddresses iterates over all network addresses of g
// and passes each of them to f.
func iterateAllAddresses(g AddressGroup, f func(Address)) {
	g.IterateAddresses(func(addr Address) bool {
		f(addr)
		return false
	})
}

// Len returns number of addresses in AddressGroup.
func (x AddressGroup) Len() int {
	return len(x)
}

// Less returns true if i-th address in AddressGroup supports TLS
// and j-th one doesn't.
func (x AddressGroup) Less(i, j int) bool {
	return x[i].TLSEnabled() && !x[j].TLSEnabled()
}

// Swap swaps i-th and j-th addresses in AddressGroup.
func (x AddressGroup) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

// MultiAddressIterator is an interface of network address group.
type MultiAddressIterator interface {
	// Must iterate over network addresses and pass each one
	// to the handler until it returns true.
	IterateAddresses(func(string) bool)

	// Must return number of addresses in group.
	NumberOfAddresses() int
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

	if cap(as) >= addrNum {
		as = as[:0]
	} else {
		as = make(AddressGroup, 0, addrNum)
	}

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
func iterateParsedAddresses(iter MultiAddressIterator, f func(s Address) error) (err error) {
	iter.IterateAddresses(func(s string) bool {
		var a Address

		err = a.FromString(s)
		if err != nil {
			err = fmt.Errorf("could not parse address from string: %w", err)
			return true
		}

		err = f(a)

		return err != nil
	})

	return
}

// WriteToNodeInfo writes AddressGroup to netmap.NodeInfo structure.
func WriteToNodeInfo(g AddressGroup, ni *netmap.NodeInfo) {
	num := g.Len()
	addrs := make([]string, 0, num)

	iterateAllAddresses(g, func(addr Address) {
		addrs = append(addrs, addr.String())
	})

	ni.SetAddresses(addrs...)
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
