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

	rem int

	nextI, nextJ int

	vectors []netmap.Nodes
}

type cfg struct {
	rem int

	addr *object.Address

	policy *netmap.PlacementPolicy

	builder Builder
}

var errNilBuilder = errors.New("placement builder is nil")

func defaultCfg() *cfg {
	return &cfg{
		rem:  1,
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
		return nil, errors.Wrap(errNilBuilder, "incomplete traverser options")
	}

	ns, err := cfg.builder.BuildPlacement(cfg.addr, cfg.policy)
	if err != nil {
		return nil, errors.Wrap(err, "could not build placement")
	}

	return &Traverser{
		mtx:     new(sync.RWMutex),
		rem:     cfg.rem,
		vectors: ns,
	}, nil
}

// Next returns next unprocessed address of the object placement.
//
// Returns nil if no nodes left or traversal operation succeeded.
func (t *Traverser) Next() *network.Address {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.rem == 0 || t.nextI == len(t.vectors) {
		return nil
	}

	addr, err := network.AddressFromString(t.vectors[t.nextI][t.nextJ].NetworkAddress())
	if err != nil {
		// TODO: log error
	}

	if t.nextJ++; t.nextJ == len(t.vectors[t.nextI]) {
		t.nextJ = 0
		t.nextI++
	}

	return addr
}

// SubmitSuccess writes single succeeded node operation.
func (t *Traverser) SubmitSuccess() {
	t.mtx.Lock()
	t.rem--
	t.mtx.Unlock()
}

// Success returns true if traversal operation succeeded.
func (t *Traverser) Success() bool {
	t.mtx.RLock()
	s := t.rem <= 0
	t.mtx.RUnlock()

	return s
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

		c.rem = 0
		for _, r := range c.policy.GetReplicas() {
			c.rem += int(r.GetCount())
		}

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
