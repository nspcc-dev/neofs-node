package common

import (
	"bytes"

	"github.com/nspcc-dev/hrw"
	apiNetmap "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// managerBuilder is implementation of reputation ManagerBuilder interface.
// It sorts nodes in NetMap with HRW algorithms and
// takes the next node after the current one as the only manager.
type managerBuilder struct {
	log   *logger.Logger
	nmSrc netmapcore.Source
	opts  *mngOptions
}

// ManagersPrm groups the required parameters of the managerBuilder's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type ManagersPrm struct {
	NetMapSource netmapcore.Source
}

// NewManagerBuilder creates a new instance of the managerBuilder.
//
// Panics if at least one value of the parameters is invalid.
//
// The created managerBuilder does not require additional
// initialization and is completely ready for work.
func NewManagerBuilder(prm ManagersPrm, opts ...MngOption) common.ManagerBuilder {
	switch {
	case prm.NetMapSource == nil:
		PanicOnPrmValue("NetMapSource", prm.NetMapSource)
	}

	o := defaultMngOpts()

	for i := range opts {
		opts[i](o)
	}

	return &managerBuilder{
		log:   o.log,
		nmSrc: prm.NetMapSource,
		opts:  o,
	}
}

// BuildManagers sorts nodes in NetMap with HRW algorithms and
// takes the next node after the current one as the only manager.
func (mb *managerBuilder) BuildManagers(epoch uint64, p reputation.PeerID) ([]common.ServerInfo, error) {
	nm, err := mb.nmSrc.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, err
	}

	// make a copy to keep order consistency of the origin netmap after sorting
	nodes := make([]*apiNetmap.Node, len(nm.Nodes))

	copy(nodes, nm.Nodes)

	hrw.SortSliceByValue(nodes, epoch)

	for i := range nodes {
		if bytes.Equal(nodes[i].PublicKey(), p.Bytes()) {
			managerIndex := i + 1

			if managerIndex == len(nodes) {
				managerIndex = 0
			}

			return []common.ServerInfo{nodes[managerIndex]}, nil
		}
	}

	return nil, nil
}

type mngOptions struct {
	log *logger.Logger
}

type MngOption func(*mngOptions)

func defaultMngOpts() *mngOptions {
	return &mngOptions{
		log: zap.L(),
	}
}

// WithLogger returns MngOption to specify logging component.
func WithLogger(l *logger.Logger) MngOption {
	return func(o *mngOptions) {
		if l != nil {
			o.log = l
		}
	}
}
