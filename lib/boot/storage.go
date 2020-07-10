package boot

import (
	"context"

	"go.uber.org/zap"
)

// StorageBootParams is a group of parameters
// for storage node bootstrap operation.
type StorageBootParams struct {
	BootstrapPeerParams
}

// StorageBootController is an entity that performs
// registration of a storage node in NeoFS network.
type StorageBootController struct {
	peerBoot PeerBootstrapper

	bootPrm StorageBootParams

	log *zap.Logger
}

// SetPeerBootstrapper is a PeerBootstrapper setter.
func (s *StorageBootController) SetPeerBootstrapper(v PeerBootstrapper) {
	s.peerBoot = v
}

// SetBootParams is a storage node bootstrap parameters setter.
func (s *StorageBootController) SetBootParams(v StorageBootParams) {
	s.bootPrm = v
}

// SetLogger is a logging component setter.
func (s *StorageBootController) SetLogger(v *zap.Logger) {
	s.log = v
}

// Bootstrap registers storage node in NeoFS system.
func (s StorageBootController) Bootstrap(context.Context) {
	// register peer in NeoFS network
	if err := s.peerBoot.AddPeer(s.bootPrm.BootstrapPeerParams); err != nil && s.log != nil {
		s.log.Error("could not register storage node in network")
	}
}
