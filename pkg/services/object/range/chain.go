package rangesvc

import (
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

type rangeTraverser struct {
	chain *rangeChain

	seekBounds *rangeBounds
}

type rangeBounds struct {
	left, right uint64
}

type objectRange struct {
	rng *objectSDK.Range

	id *objectSDK.ID
}

type rangeChain struct {
	next, prev *rangeChain

	bounds *rangeBounds

	id *objectSDK.ID
}

func newRangeTraverser(originSize uint64, rightElement *object.Object, rngSeek *objectSDK.Range) *rangeTraverser {
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

	return &rangeTraverser{
		chain: right,
		seekBounds: &rangeBounds{
			left:  rngSeek.GetOffset(),
			right: rngSeek.GetOffset() + rngSeek.GetLength(),
		},
	}
}

func (c *rangeTraverser) next() *objectRange {
	left := c.chain.bounds.left
	seekLeft := c.seekBounds.left

	res := new(objectRange)

	if left > seekLeft {
		res.id = c.chain.prev.id
	} else {
		res.id = c.chain.id
		res.rng = objectSDK.NewRange()
		res.rng.SetOffset(seekLeft - left)
		res.rng.SetLength(min(c.chain.bounds.right, c.seekBounds.right) - seekLeft)
	}

	return res
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

func (c *rangeTraverser) pushHeader(obj *object.Object) {
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

	c.chain.prev = &rangeChain{
		next: c.chain,
		id:   obj.GetPreviousID(),
	}
}

func (c *rangeTraverser) pushSuccessSize(sz uint64) {
	c.seekBounds.left += sz

	if c.seekBounds.left >= c.chain.bounds.right && c.chain.next != nil {
		c.chain = c.chain.next
	}
}
