package util

import (
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

func NewRangeTraverser(originSize uint64, rightElement *object.Object, rngSeek *objectSDK.Range) *RangeTraverser {
	right := &rangeChain{
		bounds: &rangeBounds{
			left:  originSize - rightElement.GetPayloadSize(),
			right: originSize,
		},
		id: rightElement.GetID(),
	}

	left := &rangeChain{
		id: rightElement.GetPreviousID(),
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
	id := obj.GetID()
	if !id.Equal(c.chain.prev.id) {
		panic(fmt.Sprintf("(%T) unexpected identifier in header", c))
	}

	sz := obj.GetPayloadSize()

	c.chain.prev.bounds = &rangeBounds{
		left:  c.chain.bounds.left - sz,
		right: c.chain.bounds.left,
	}

	c.chain = c.chain.prev

	if prev := obj.GetPreviousID(); prev != nil {
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
