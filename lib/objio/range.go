package objio

import (
	"context"
	"io"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/pkg/errors"
)

type (
	// Address is a type alias of
	// Address from refs package of neofs-api-go.
	Address = refs.Address

	// ChopperTable is an interface of RangeChopper storage.
	ChopperTable interface {
		PutChopper(addr Address, chopper RangeChopper) error
		GetChopper(addr Address, rc RCType) (RangeChopper, error)
	}

	// RangeChopper is an interface of the chooper of object payload range.
	RangeChopper interface {
		GetType() RCType
		GetAddress() Address
		Closed() bool
		Chop(ctx context.Context, length, offset int64, fromStart bool) ([]RangeDescriptor, error)
	}

	// RelativeReceiver is an interface of object relations controller.
	RelativeReceiver interface {
		Base(ctx context.Context, addr Address) (RangeDescriptor, error)
		Neighbor(ctx context.Context, addr Address, left bool) (RangeDescriptor, error)
	}

	// ChildLister is an interface of object children info storage.
	ChildLister interface {
		List(ctx context.Context, parent Address) ([]RangeDescriptor, error)
	}

	// RangeDescriptor groups the information about object payload range.
	RangeDescriptor struct {
		Size   int64
		Offset int64
		Addr   Address

		LeftBound  bool
		RightBound bool
	}

	chopCache struct {
		rangeList []RangeDescriptor
	}

	chopper struct {
		*sync.RWMutex
		ct          RCType
		addr        Address
		nr          RelativeReceiver
		cacheOffset int64
		cache       *chopCache
	}

	// ChopperParams groups the parameters of Scylla chopper.
	ChopperParams struct {
		RelativeReceiver RelativeReceiver
		Addr             Address
	}

	charybdis struct {
		skr *chopper
		cl  ChildLister
	}

	// CharybdisParams groups the parameters of Charybdis chopper.
	CharybdisParams struct {
		Addr        Address
		ChildLister ChildLister

		ReadySelection []RangeDescriptor
	}

	// RCType is an enumeration of object payload range chopper types.
	RCType int

	chopperTable struct {
		*sync.RWMutex
		items map[RCType]map[string]RangeChopper
	}
)

const (
	// RCScylla is an RCType of payload range post-pouncing chopper.
	RCScylla RCType = iota

	// RCCharybdis is an RCType of payload range pre-pouncing chopper.
	RCCharybdis
)

var errNilRelativeReceiver = errors.New("relative receiver is nil")

var errEmptyObjectID = errors.New("object ID is empty")

var errNilChildLister = errors.New("child lister is nil")

var errNotFound = errors.New("object range chopper not found")

var errInvalidBound = errors.New("invalid payload bounds")

// NewChopperTable is a RangeChopper storage constructor.
func NewChopperTable() ChopperTable {
	return &chopperTable{
		new(sync.RWMutex),
		make(map[RCType]map[string]RangeChopper),
	}
}

// NewScylla constructs object payload range chopper that collects parts of a range on the go.
func NewScylla(p *ChopperParams) (RangeChopper, error) {
	if p.RelativeReceiver == nil {
		return nil, errNilRelativeReceiver
	}

	if p.Addr.ObjectID.Empty() {
		return nil, errEmptyObjectID
	}

	return &chopper{
		RWMutex: new(sync.RWMutex),
		ct:      RCScylla,
		nr:      p.RelativeReceiver,
		addr:    p.Addr,
		cache: &chopCache{
			rangeList: make([]RangeDescriptor, 0),
		},
	}, nil
}

// NewCharybdis constructs object payload range that pre-collects all parts of the range.
func NewCharybdis(p *CharybdisParams) (RangeChopper, error) {
	if p.ChildLister == nil && len(p.ReadySelection) == 0 {
		return nil, errNilChildLister
	}

	if p.Addr.ObjectID.Empty() {
		return nil, errEmptyObjectID
	}

	cache := new(chopCache)

	if len(p.ReadySelection) > 0 {
		cache.rangeList = p.ReadySelection
	}

	return &charybdis{
		skr: &chopper{
			RWMutex: new(sync.RWMutex),
			ct:      RCCharybdis,
			addr:    p.Addr,
			cache:   cache,
		},
		cl: p.ChildLister,
	}, nil
}

func (ct *chopperTable) PutChopper(addr Address, chopper RangeChopper) error {
	ct.Lock()
	defer ct.Unlock()

	sAddr := addr.String()
	chopperType := chopper.GetType()

	m, ok := ct.items[chopperType]
	if !ok {
		m = make(map[string]RangeChopper)
	}

	if _, ok := m[sAddr]; !ok {
		m[sAddr] = chopper
	}

	ct.items[chopperType] = m

	return nil
}

func (ct *chopperTable) GetChopper(addr Address, rc RCType) (RangeChopper, error) {
	ct.Lock()
	defer ct.Unlock()

	choppers, ok := ct.items[rc]
	if !ok {
		return nil, errNotFound
	}

	chp, ok := choppers[addr.String()]
	if !ok {
		return nil, errNotFound
	}

	return chp, nil
}

func (c charybdis) GetAddress() Address {
	return c.skr.addr
}

func (c charybdis) GetType() RCType {
	return c.skr.ct
}

func (c charybdis) Closed() bool {
	return len(c.skr.cache.rangeList) > 0
}

func (c *charybdis) devour(ctx context.Context) error {
	if len(c.skr.cache.rangeList) == 0 {
		rngs, err := c.cl.List(ctx, c.skr.addr)
		if err != nil {
			return errors.Wrap(err, "charybdis.pounce faild on children list")
		}

		if ln := len(rngs); ln > 0 {
			rngs[0].LeftBound = true
			rngs[ln-1].RightBound = true
		}

		c.skr.cache.rangeList = rngs
	}

	return nil
}

func (c *charybdis) Chop(ctx context.Context, length, offset int64, fromStart bool) ([]RangeDescriptor, error) {
	if err := c.devour(ctx); err != nil {
		return nil, errors.Wrap(err, "charybdis.Chop failed on devour")
	}

	return c.skr.Chop(ctx, length, offset, fromStart)
}

func (sc *chopCache) Size() (res int64) {
	for i := range sc.rangeList {
		res += sc.rangeList[i].Size
	}

	return
}

func (sc *chopCache) touchStart() bool {
	return len(sc.rangeList) > 0 && sc.rangeList[0].LeftBound
}

func (sc *chopCache) touchEnd() bool {
	ln := len(sc.rangeList)

	return ln > 0 && sc.rangeList[ln-1].RightBound
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

func (sc *chopCache) Chop(offset, size int64) ([]RangeDescriptor, error) {
	if offset*size < 0 {
		return nil, errInvalidBound
	}

	if offset+size > sc.Size() {
		return nil, localstore.ErrOutOfRange
	}

	var (
		off         int64
		res         = make([]RangeDescriptor, 0)
		ind         int
		firstOffset int64
	)

	for i := range sc.rangeList {
		diff := offset - off
		if diff > sc.rangeList[i].Size {
			off += sc.rangeList[i].Size
			continue
		} else if diff < sc.rangeList[i].Size {
			ind = i
			firstOffset = diff
			break
		}

		ind = i + 1

		break
	}

	var (
		r   RangeDescriptor
		num int64
	)

	for i := ind; num < size; i++ {
		cut := min(size-num, sc.rangeList[i].Size-firstOffset)
		r = RangeDescriptor{
			Size: cut,
			Addr: sc.rangeList[i].Addr,

			LeftBound:  sc.rangeList[i].LeftBound,
			RightBound: sc.rangeList[i].RightBound,
		}

		if i == ind {
			r.Offset = firstOffset
			firstOffset = 0
		}

		if cut == size-num {
			r.Size = cut
		}

		res = append(res, r)

		num += cut
	}

	return res, nil
}

func (c *chopper) GetAddress() Address {
	return c.addr
}

func (c *chopper) GetType() RCType {
	return c.ct
}

func (c *chopper) Closed() bool {
	return c.cache.touchStart() && c.cache.touchEnd()
}

func (c *chopper) pounce(ctx context.Context, off int64, set bool) error {
	if len(c.cache.rangeList) == 0 {
		child, err := c.nr.Base(ctx, c.addr)
		if err != nil {
			return errors.Wrap(err, "chopper.pounce failed on cache init")
		}

		c.cache.rangeList = []RangeDescriptor{child}
	}

	oldOff := c.cacheOffset

	defer func() {
		if !set {
			c.cacheOffset = oldOff
		}
	}()

	var (
		cacheSize = c.cache.Size()
		v         = c.cacheOffset + off
	)

	switch {
	case v >= 0 && v <= cacheSize:
		c.cacheOffset = v
		return nil
	case v < 0 && c.cache.touchStart():
		c.cacheOffset = 0
		return io.EOF
	case v > cacheSize && c.cache.touchEnd():
		c.cacheOffset = cacheSize
		return io.EOF
	}

	var (
		alloc, written int64
		toLeft         = v < 0
		procAddr       Address
		fPush          = func(r RangeDescriptor) {
			if toLeft {
				c.cache.rangeList = append([]RangeDescriptor{r}, c.cache.rangeList...)
				return
			}
			c.cache.rangeList = append(c.cache.rangeList, r)
		}
	)

	if toLeft {
		alloc = -v
		procAddr = c.cache.rangeList[0].Addr
		c.cacheOffset -= cacheSize
	} else {
		alloc = v - cacheSize
		procAddr = c.cache.rangeList[len(c.cache.rangeList)-1].Addr
		c.cacheOffset += cacheSize
	}

	for written < alloc {
		rng, err := c.nr.Neighbor(ctx, procAddr, toLeft)
		if err != nil {
			return errors.Wrap(err, "chopper.pounce failed on get neighbor")
		}

		if diff := alloc - written; diff < rng.Size {
			if toLeft {
				rng.Offset = rng.Size - diff
			}

			c.cacheOffset += diff

			fPush(rng)

			break
		}

		c.cacheOffset += rng.Size
		fPush(rng)

		written += rng.Size

		if written < alloc &&
			(rng.LeftBound && toLeft || rng.RightBound && !toLeft) {
			return localstore.ErrOutOfRange
		}

		procAddr = rng.Addr
	}

	return nil
}

func (c *chopper) Chop(ctx context.Context, length, offset int64, fromStart bool) ([]RangeDescriptor, error) {
	c.Lock()
	defer c.Unlock()

	if fromStart {
		if err := c.pounce(ctx, -(1 << 63), true); err != nil && err != io.EOF {
			return nil, errors.Wrap(err, "chopper.Chop failed on chopper.pounce to start")
		}
	}

	if err := c.pounce(ctx, offset, true); err != nil && err != io.EOF {
		return nil, errors.Wrap(err, "chopper.Chop failed on chopper.pounce with set")
	}

	if c.cache.Size()-c.cacheOffset < length {
		if err := c.pounce(ctx, length, false); err != nil && err != io.EOF {
			return nil, errors.Wrap(err, "chopper.Chop failed on chopper.pounce")
		}
	}

	return c.cache.Chop(c.cacheOffset, length)
}
