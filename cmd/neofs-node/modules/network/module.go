package network

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/module"
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
)

// Module is a network layer module.
var Module = module.Module{
	{Constructor: newMuxer},
	{Constructor: newPeers},
	{Constructor: newPlacement},

	// Metrics is prometheus handler
	{Constructor: profiler.NewMetrics},
	// Profiler is pprof handler
	{Constructor: profiler.NewProfiler},
	{Constructor: newHTTPHandler},
}
