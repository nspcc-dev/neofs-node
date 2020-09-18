package placement

import (
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/pkg/errors"
)

// Builder is an interface of the
// object placement vector builder.
type Builder interface {
	// BuildPlacement returns the list of placement vectors
	// for object according to the placement policy.
	//
	// Must return all container nodes if object identifier
	// is nil.
	BuildPlacement(*object.Address, *netmap.PlacementPolicy) ([]netmap.Nodes, error)
}

// Option represents placement traverser option.
type Option func(*cfg)

// Traverser represents utility for controlling
// traversal of object placement vectors.
type Traverser struct {
	mtx *sync.RWMutex

	vectors []netmap.Nodes

	rem []int
}

type cfg struct {
	rem int

	addr *object.Address

	policy *netmap.PlacementPolicy

	builder Builder
}

const invalidOptsMsg = "invalid traverser options"

var errNilBuilder = errors.New("placement builder is nil")

var errNilPolicy = errors.New("placement policy is nil")

func defaultCfg() *cfg {
	return &cfg{
		addr: object.NewAddress(),
	}
}

// NewTraverser creates, initializes with options and returns Traverser instance.
func NewTraverser(opts ...Option) (*Traverser, error) {
	cfg := defaultCfg()

	for i := range opts {
		if opts[i] != nil {
			opts[i](cfg)
		}
	}

	if cfg.builder == nil {
		return nil, errors.Wrap(errNilBuilder, invalidOptsMsg)
	} else if cfg.policy == nil {
		return nil, errors.Wrap(errNilPolicy, invalidOptsMsg)
	}

	ns, err := cfg.builder.BuildPlacement(cfg.addr, cfg.policy)
	if err != nil {
		return nil, errors.Wrap(err, "could not build placement")
	}

	ss := cfg.policy.GetSelectors()
	rem := make([]int, 0, len(ss))

	for i := range ss {
		cnt := cfg.rem

		if cnt == 0 {
			cnt = int(ss[i].GetCount())
		}

		rem = append(rem, cnt)
	}

	return &Traverser{
		mtx:     new(sync.RWMutex),
		rem:     rem,
		vectors: ns,
	}, nil
}

// Next returns next unprocessed address of the object placement.
//
// Returns nil if no nodes left or traversal operation succeeded.
func (t *Traverser) Next() []*network.Address {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	t.skipEmptyVectors()

	if len(t.vectors) == 0 {
		return nil
	} else if len(t.vectors[0]) < t.rem[0] {
		return nil
	}

	count := t.rem[0]
	if count < 0 {
		count = len(t.vectors[0])
	}

	addrs := make([]*network.Address, 0, count)

	for i := 0; i < count; i++ {
		addr, err := network.AddressFromString(t.vectors[0][i].NetworkAddress())
		if err != nil {
			// TODO: log error
			return nil
		}

		addrs = append(addrs, addr)
	}

	t.vectors[0] = t.vectors[0][count:]

	return addrs
}

func (t *Traverser) skipEmptyVectors() {
	for i := 0; i < len(t.vectors); i++ { // don't use range, slice changes in body
		if len(t.vectors[i]) == 0 && t.rem[i] <= 0 || t.rem[0] == 0 {
			t.vectors = append(t.vectors[:i], t.vectors[i+1:]...)
			t.rem = append(t.rem[:i], t.rem[i+1:]...)
			i--
		} else {
			break
		}
	}
}

// SubmitSuccess writes single succeeded node operation.
func (t *Traverser) SubmitSuccess() {
	t.mtx.Lock()
	if len(t.rem) > 0 {
		t.rem[0]--
	}
	t.mtx.Unlock()
}

// Success returns true if traversal operation succeeded.
func (t *Traverser) Success() bool {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	for i := range t.rem {
		if t.rem[i] > 0 {
			return false
		}
	}

	return true
}

// UseBuilder is a placement builder setting option.
//
// Overlaps UseNetworkMap option.
func UseBuilder(b Builder) Option {
	return func(c *cfg) {
		c.builder = b
	}
}

// UseNetworkMap is a placement builder based on network
// map setting option.
//
// Overlaps UseBuilder option.
func UseNetworkMap(nm *netmap.Netmap) Option {
	return func(c *cfg) {
		c.builder = &netMapBuilder{
			nm: nm,
		}
	}
}

// ForContainer is a traversal container setting option.
func ForContainer(cnr *container.Container) Option {
	return func(c *cfg) {
		c.policy = cnr.GetPlacementPolicy()
		c.addr.SetContainerID(container.CalculateID(cnr))
	}
}

// ForObject is a processing object setting option.
func ForObject(id *object.ID) Option {
	return func(c *cfg) {
		c.addr.SetObjectID(id)
	}
}

// SuccessAfter is a success number setting option.
//
// Option has no effect if the number is not positive.
func SuccessAfter(v int) Option {
	return func(c *cfg) {
		if v > 0 {
			c.rem = v
		}
	}
}

// WithoutSuccessTracking disables success tracking in traversal.
func WithoutSuccessTracking() Option {
	return func(c *cfg) {
		c.rem = -1
	}
}
