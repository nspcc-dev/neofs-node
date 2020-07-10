package boot

import (
	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-node/internal"
)

// BootstrapPeerParams is a group of parameters
// for storage node bootstrap.
type BootstrapPeerParams struct {
	info *bootstrap.NodeInfo
}

// PeerBootstrapper is an interface of the NeoFS node bootstrap tool.
type PeerBootstrapper interface {
	AddPeer(BootstrapPeerParams) error
}

// ErrNilPeerBootstrapper is returned by functions that expect
// a non-nil PeerBootstrapper, but received nil.
const ErrNilPeerBootstrapper = internal.Error("peer bootstrapper is nil")

// SetNodeInfo is a node info setter.
func (s *BootstrapPeerParams) SetNodeInfo(v *bootstrap.NodeInfo) {
	s.info = v
}

// NodeInfo is a node info getter.
func (s BootstrapPeerParams) NodeInfo() *bootstrap.NodeInfo {
	return s.info
}
