package util

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// HeadReceiver is an interface of entity that can receive
// object header or the information about the object relations.
type HeadReceiver interface {
	// Head must return one of:
	// * object header (*object.Object);
	// * structured information about split-chain (*object.SplitInfo).
	Head(id oid.Address) (any, error)
}

// SplitMemberHandler is a handler of next split-chain element.
//
// If reverseDirection arg is true, then the traversal is done in reverse order.
// Stop boolean result provides the ability to interrupt the traversal.
type SplitMemberHandler func(member *object.Object, reverseDirection bool) (stop bool)

// IterateAllSplitLeaves is an iterator over all object split-tree leaves in direct order.
func IterateAllSplitLeaves(r HeadReceiver, addr oid.Address, h func(*object.Object)) error {
	return IterateSplitLeaves(r, addr, func(leaf *object.Object) bool {
		h(leaf)
		return false
	})
}

// IterateSplitLeaves is an iterator over object split-tree leaves in direct order.
//
// If member handler returns true, then the iterator aborts without error.
func IterateSplitLeaves(r HeadReceiver, addr oid.Address, h func(*object.Object) bool) error {
	var (
		reverse bool
		leaves  []*object.Object
	)

	if err := TraverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
		reverse = reverseDirection

		if reverse {
			leaves = append(leaves, member)
			return false
		}

		return h(member)
	}); err != nil {
		return err
	}

	for i := len(leaves) - 1; i >= 0; i-- {
		if h(leaves[i]) {
			break
		}
	}

	return nil
}

// TraverseSplitChain is an iterator over object split-tree leaves.
//
// Traversal occurs in one of two directions, which depends on what pslit info was received:
// * in direct order for link part;
// * in reverse order for last part.
func TraverseSplitChain(r HeadReceiver, addr oid.Address, h SplitMemberHandler) error {
	_, err := traverseSplitChain(r, addr, h)
	return err
}

func traverseSplitChain(r HeadReceiver, addr oid.Address, h SplitMemberHandler) (bool, error) {
	v, err := r.Head(addr)
	if err != nil {
		return false, err
	}

	cnr := addr.Container()

	switch res := v.(type) {
	default:
		panic(fmt.Sprintf("unexpected result of %T: %T", r, v))
	case *object.Object:
		return h(res, false), nil
	case *object.SplitInfo:
		link, withLink := res.Link()
		last, withLast := res.LastPart()

		switch {
		default:
			return false, errors.New("lack of split information")
		case withLink:
			var addr oid.Address
			addr.SetContainer(cnr)
			addr.SetObject(link)

			chain := make([]oid.ID, 0)

			if _, err := traverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
				children := member.Children()

				if reverseDirection {
					chain = append(children, chain...)
				} else {
					chain = append(chain, children...)
				}

				return false
			}); err != nil {
				return false, err
			}

			var reverseChain []*object.Object

			for i := range chain {
				addr.SetObject(chain[i])

				if stop, err := traverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
					if !reverseDirection {
						return h(member, false)
					}

					reverseChain = append(reverseChain, member)
					return false
				}); err != nil || stop {
					return stop, err
				}
			}

			for i := len(reverseChain) - 1; i >= 0; i-- {
				if h(reverseChain[i], false) {
					return true, nil
				}
			}
		case withLast:
			var addr oid.Address
			addr.SetContainer(cnr)

			for last, withLast = res.LastPart(); withLast; {
				addr.SetObject(last)

				var directChain []*object.Object

				if _, err := traverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
					if reverseDirection {
						last, withLast = member.PreviousID()
						return h(member, true)
					}

					directChain = append(directChain, member)

					return false
				}); err != nil {
					return false, err
				}

				for i := len(directChain) - 1; i >= 0; i-- {
					if h(directChain[i], true) {
						return true, nil
					}
				}

				if len(directChain) > 0 {
					last, withLast = directChain[len(directChain)-1].PreviousID()
				}
			}
		}
	}

	return false, nil
}
