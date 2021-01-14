package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
)

// NetmapSnapshot reads network map snapshot from Netmap storage.
func (s *Server) NetmapSnapshot(ctx context.Context, req *control.NetmapSnapshotRequest) (*control.NetmapSnapshotResponse, error) {
	panic("implement me")
}
