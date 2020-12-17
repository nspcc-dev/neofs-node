package util

import (
	"errors"
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

type RangeTraverser struct {
	chain *rangeChain

	seekBounds *rangeBounds
}

type rangeBounds struct {
	left, right uint64
}

type rangeChain struct {
	next, prev *rangeChain

	bounds *rangeBounds

	id *objectSDK.ID
}

// HeadReceiver is an interface of entity that can receive
// object header or the information about the object relations.
type HeadReceiver interface {
	// Head must return one of:
	// * object header (*object.Object);
	// * structured information about split-chain (*objectSDK.SplitInfo).
	Head(*objectSDK.Address) (interface{}, error)
}

// SplitMemberHandler is a handler of next split-chain element.
//
// If reverseDirection arg is true, then the traversal is done in reverse order.
// Stop boolean result provides the ability to interrupt the traversal.
type SplitMemberHandler func(member *object.Object, reverseDirection bool) (stop bool)

// IterateAllSplitLeaves is an iterator over all object split-tree leaves in direct order.
func IterateAllSplitLeaves(r HeadReceiver, addr *objectSDK.Address, h func(*object.Object)) error {
	return IterateSplitLeaves(r, addr, func(leaf *object.Object) bool {
		h(leaf)
		return false
	})
}

// IterateSplitLeaves is an iterator over object split-tree leaves in direct order.
//
// If member handler returns true, then the iterator aborts without error.
func IterateSplitLeaves(r HeadReceiver, addr *objectSDK.Address, h func(*object.Object) bool) error {
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
func TraverseSplitChain(r HeadReceiver, addr *objectSDK.Address, h SplitMemberHandler) error {
	_, err := traverseSplitChain(r, addr, h)
	return err
}

func traverseSplitChain(r HeadReceiver, addr *objectSDK.Address, h SplitMemberHandler) (bool, error) {
	v, err := r.Head(addr)
	if err != nil {
		return false, err
	}

	cid := addr.ContainerID()

	switch res := v.(type) {
	default:
		panic(fmt.Sprintf("unexpected result of %T: %T", r, v))
	case *object.Object:
		return h(res, false), nil
	case *objectSDK.SplitInfo:
		switch {
		default:
			return false, errors.New("lack of split information")
		case res.Link() != nil:
			addr := objectSDK.NewAddress()
			addr.SetContainerID(cid)
			addr.SetObjectID(res.Link())

			chain := make([]*objectSDK.ID, 0)

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
				addr.SetObjectID(chain[i])

				if stop, err := traverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
					if !reverseDirection {
						return h(member, false)
					} else {
						reverseChain = append(reverseChain, member)
						return false
					}
				}); err != nil || stop {
					return stop, err
				}
			}

			for i := len(reverseChain) - 1; i >= 0; i-- {
				if h(reverseChain[i], false) {
					return true, nil
				}
			}
		case res.LastPart() != nil:
			addr := objectSDK.NewAddress()
			addr.SetContainerID(cid)

			for prev := res.LastPart(); prev != nil; {
				addr.SetObjectID(prev)

				var directChain []*object.Object

				if _, err := traverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
					if reverseDirection {
						prev = member.PreviousID()
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
					prev = directChain[len(directChain)-1].PreviousID()
				}
			}
		}
	}

	return false, nil
}

func NewRangeTraverser(originSize uint64, rightElement *object.Object, rngSeek *objectSDK.Range) *RangeTraverser {
	right := &rangeChain{
		bounds: &rangeBounds{
			left:  originSize - rightElement.PayloadSize(),
			right: originSize,
		},
		id: rightElement.ID(),
	}

	left := &rangeChain{
		id: rightElement.PreviousID(),
	}

	left.next, right.prev = right, left

	return &RangeTraverser{
		chain: right,
		seekBounds: &rangeBounds{
			left:  rngSeek.GetOffset(),
			right: rngSeek.GetOffset() + rngSeek.GetLength(),
		},
	}
}

func (c *RangeTraverser) Next() (id *objectSDK.ID, rng *objectSDK.Range) {
	left := c.chain.bounds.left
	seekLeft := c.seekBounds.left

	if left > seekLeft {
		id = c.chain.prev.id
	} else {
		id = c.chain.id
		rng = objectSDK.NewRange()
		rng.SetOffset(seekLeft - left)
		rng.SetLength(min(c.chain.bounds.right, c.seekBounds.right) - seekLeft)
	}

	return
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

func (c *RangeTraverser) PushHeader(obj *object.Object) {
	id := obj.ID()
	if !id.Equal(c.chain.prev.id) {
		panic(fmt.Sprintf("(%T) unexpected identifier in header", c))
	}

	sz := obj.PayloadSize()

	c.chain.prev.bounds = &rangeBounds{
		left:  c.chain.bounds.left - sz,
		right: c.chain.bounds.left,
	}

	c.chain = c.chain.prev

	if prev := obj.PreviousID(); prev != nil {
		c.chain.prev = &rangeChain{
			next: c.chain,
			id:   prev,
		}
	}
}

func (c *RangeTraverser) PushSuccessSize(sz uint64) {
	c.seekBounds.left += sz

	if c.seekBounds.left >= c.chain.bounds.right && c.chain.next != nil {
		c.chain = c.chain.next
	}
}

// SetSeekRange moves the chain to the specified range.
// The range is expected to be within the filled chain.
func (c *RangeTraverser) SetSeekRange(r *objectSDK.Range) {
	ln, off := r.GetLength(), r.GetOffset()

	for {
		if off < c.chain.bounds.left {
			if c.chain.prev == nil {
				break
			}

			c.chain = c.chain.prev
		} else if off >= c.chain.bounds.left && off < c.chain.bounds.right {
			break
		} else if off >= c.chain.bounds.right {
			if c.chain.next == nil {
				break
			}

			c.chain = c.chain.next
		}
	}

	if c.seekBounds == nil {
		c.seekBounds = new(rangeBounds)
	}

	c.seekBounds.left, c.seekBounds.right = off, off+ln
}
