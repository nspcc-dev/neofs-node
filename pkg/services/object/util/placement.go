package util

import (
	netmapSDK "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/pkg/errors"
)

type localPlacement struct {
	builder placement.Builder

	localAddrSrc network.LocalAddressSource
}

type remotePlacement struct {
	builder placement.Builder

	localAddrSrc network.LocalAddressSource
}

// TraverserGenerator represents tool that generates
// container traverser for the particular need.
type TraverserGenerator struct {
	netMapSrc netmap.Source

	cnrSrc container.Source

	localAddrSrc network.LocalAddressSource

	customOpts []placement.Option
}

func NewLocalPlacement(b placement.Builder, s network.LocalAddressSource) placement.Builder {
	return &localPlacement{
		builder:      b,
		localAddrSrc: s,
	}
}

func (p *localPlacement) BuildPlacement(addr *object.Address, policy *netmapSDK.PlacementPolicy) ([]netmapSDK.Nodes, error) {
	vs, err := p.builder.BuildPlacement(addr, policy)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not build object placement", p)
	}

	for i := range vs {
		for j := range vs[i] {
			addr, err := network.AddressFromString(vs[i][j].Address())
			if err != nil {
				// TODO: log error
				continue
			}

			if network.IsLocalAddress(p.localAddrSrc, addr) {
				return []netmapSDK.Nodes{{vs[i][j]}}, nil
			}
		}
	}

	return nil, errors.Errorf("(%T) local node is outside of object placement", p)
}

// NewRemotePlacementBuilder creates, initializes and returns placement builder that
// excludes local node from any placement vector.
func NewRemotePlacementBuilder(b placement.Builder, s network.LocalAddressSource) placement.Builder {
	return &remotePlacement{
		builder:      b,
		localAddrSrc: s,
	}
}

func (p *remotePlacement) BuildPlacement(addr *object.Address, policy *netmapSDK.PlacementPolicy) ([]netmapSDK.Nodes, error) {
	vs, err := p.builder.BuildPlacement(addr, policy)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not build object placement", p)
	}

	for i := range vs {
		for j := 0; j < len(vs[i]); j++ {
			addr, err := network.AddressFromString(vs[i][j].Address())
			if err != nil {
				// TODO: log error
				continue
			}

			if network.IsLocalAddress(p.localAddrSrc, addr) {
				vs[i] = append(vs[i][:j], vs[i][j+1:]...)
				j--
			}
		}
	}

	return vs, nil
}

// NewTraverserGenerator creates, initializes and returns new TraverserGenerator instance.
func NewTraverserGenerator(nmSrc netmap.Source, cnrSrc container.Source, localAddrSrc network.LocalAddressSource) *TraverserGenerator {
	return &TraverserGenerator{
		netMapSrc:    nmSrc,
		cnrSrc:       cnrSrc,
		localAddrSrc: localAddrSrc,
	}
}

// WithTraverseOptions returns TraverseGenerator that additionally applies provided options.
func (g *TraverserGenerator) WithTraverseOptions(opts ...placement.Option) *TraverserGenerator {
	return &TraverserGenerator{
		netMapSrc:    g.netMapSrc,
		cnrSrc:       g.cnrSrc,
		localAddrSrc: g.localAddrSrc,
		customOpts:   opts,
	}
}

// GenerateTraverser generates placement Traverser for provided object address
// using epoch-th network map.
func (g *TraverserGenerator) GenerateTraverser(addr *object.Address, epoch uint64) (*placement.Traverser, error) {
	// get network map by epoch
	nm, err := g.netMapSrc.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get network map #%d", epoch)
	}

	// get container related container
	cnr, err := g.cnrSrc.Get(addr.ContainerID())
	if err != nil {
		return nil, errors.Wrap(err, "could not get container")
	}

	// allocate placement traverser options
	traverseOpts := make([]placement.Option, 0, 3+len(g.customOpts))
	traverseOpts = append(traverseOpts, g.customOpts...)

	// create builder of the remote nodes from network map
	builder := NewRemotePlacementBuilder(
		placement.NewNetworkMapBuilder(nm),
		g.localAddrSrc,
	)

	traverseOpts = append(traverseOpts,
		// set processing container
		placement.ForContainer(cnr),

		// set identifier of the processing object
		placement.ForObject(addr.ObjectID()),

		// set placement builder
		placement.UseBuilder(builder),
	)

	return placement.NewTraverser(traverseOpts...)
}
