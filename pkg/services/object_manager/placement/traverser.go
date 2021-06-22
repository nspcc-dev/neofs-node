package placement

import (
	"errors"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
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
	trackCopies bool

	flatSuccess *uint32

	addr *object.Address

	policy *netmap.PlacementPolicy

	builder Builder
}

const invalidOptsMsg = "invalid traverser options"

var errNilBuilder = errors.New("placement builder is nil")

var errNilPolicy = errors.New("placement policy is nil")

func defaultCfg() *cfg {
	return &cfg{
		trackCopies: true,
		addr:        object.NewAddress(),
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
		return nil, fmt.Errorf("%s: %w", invalidOptsMsg, errNilBuilder)
	} else if cfg.policy == nil {
		return nil, fmt.Errorf("%s: %w", invalidOptsMsg, errNilPolicy)
	}

	ns, err := cfg.builder.BuildPlacement(cfg.addr, cfg.policy)
	if err != nil {
		return nil, fmt.Errorf("could not build placement: %w", err)
	}

	var rem []int
	if cfg.flatSuccess != nil {
		ns = flatNodes(ns)
		rem = []int{int(*cfg.flatSuccess)}
	} else {
		rs := cfg.policy.Replicas()
		rem = make([]int, 0, len(rs))

		for i := range rs {
			if cfg.trackCopies {
				rem = append(rem, int(rs[i].Count()))
			} else {
				rem = append(rem, -1)
			}
		}
	}

	return &Traverser{
		mtx:     new(sync.RWMutex),
		rem:     rem,
		vectors: ns,
	}, nil
}

func flatNodes(ns []netmap.Nodes) []netmap.Nodes {
	sz := 0
	for i := range ns {
		sz += len(ns[i])
	}

	flat := make(netmap.Nodes, 0, sz)
	for i := range ns {
		flat = append(flat, ns[i]...)
	}

	return []netmap.Nodes{flat}
}

// Next returns next unprocessed address of the object placement.
//
// Returns nil if no nodes left or traversal operation succeeded.
func (t *Traverser) Next() []network.AddressGroup {
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

	addrs := make([]network.AddressGroup, count)

	for i := 0; i < count; i++ {
		err := addrs[i].FromIterator(t.vectors[0][i])
		if err != nil {
			// TODO: log error
			return nil
		}
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

// ForContainer is a traversal container setting option.
func ForContainer(cnr *container.Container) Option {
	return func(c *cfg) {
		c.policy = cnr.PlacementPolicy()
		c.addr.SetContainerID(container.CalculateID(cnr))
	}
}

// ForObject is a processing object setting option.
func ForObject(id *object.ID) Option {
	return func(c *cfg) {
		c.addr.SetObjectID(id)
	}
}

// SuccessAfter is a flat success number setting option.
//
// Option has no effect if the number is not positive.
func SuccessAfter(v uint32) Option {
	return func(c *cfg) {
		if v > 0 {
			c.flatSuccess = &v
		}
	}
}

// WithoutSuccessTracking disables success tracking in traversal.
func WithoutSuccessTracking() Option {
	return func(c *cfg) {
		c.trackCopies = false
	}
}
