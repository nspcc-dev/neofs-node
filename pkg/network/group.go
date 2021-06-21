package network

import (
	"errors"

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

	x.IterateAddresses(func(addr Address) bool {
		s += addr.String()
		return false
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

// MultiAddressIterator is an interface of network address group.
type MultiAddressIterator interface {
	// Must iterate over network addresses and pass each one
	// to the handler until it returns true.
	IterateAddresses(func(string) bool)

	// Must return number of addresses in group.
	NumberOfAddresses() int
}

// FromIterator forms AddressGroup from MultiAddressIterator structure.
//
// Returns an error in the absence of addresses or if any of the addresses are incorrect.
func (x *AddressGroup) FromIterator(iter MultiAddressIterator) (err error) {
	as := *x

	addrNum := iter.NumberOfAddresses()
	if addrNum <= 0 {
		err = errors.New("missing network addresses")
		return
	}

	if cap(as) >= addrNum {
		as = as[:0]
	} else {
		as = make(AddressGroup, 0, addrNum)
	}

	iter.IterateAddresses(func(s string) bool {
		var a Address

		err = a.FromString(s)

		fail := err != nil
		if !fail {
			as = append(as, a)
		}

		return fail
	})

	if err == nil {
		*x = as
	}

	return
}

// GroupFromAddress wraps single Address into AddressGroup.
//
// Deprecated: use AddressGroup.FromIterator method.
func GroupFromAddress(addr Address) AddressGroup {
	return AddressGroup{addr}
}

// WriteToNodeInfo writes AddressGroup to netmap.NodeInfo structure.
func (x AddressGroup) WriteToNodeInfo(ni *netmap.NodeInfo) {
	addrs := make([]string, len(x))

	for i := range x {
		addrs[i] = x[i].String()
	}

	ni.SetAddresses(addrs...)
}

// Intersects checks if two AddressGroup have
// at least one common address.
func (x AddressGroup) Intersects(x2 AddressGroup) bool {
	for i := range x {
		for j := range x2 {
			if x[i].Equal(x2[j]) {
				return true
			}
		}
	}

	return false
}
