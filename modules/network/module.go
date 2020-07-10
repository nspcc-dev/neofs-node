package network

import (
	"github.com/nspcc-dev/neofs-node/lib/fix/module"
	"github.com/nspcc-dev/neofs-node/lib/fix/web"
)

// Module is a network layer module.
var Module = module.Module{
	{Constructor: newMuxer},
	{Constructor: newPeers},
	{Constructor: newPlacement},
	{Constructor: newTransport},

	// Metrics is prometheus handler
	{Constructor: web.NewMetrics},
	// Profiler is pprof handler
	{Constructor: web.NewProfiler},
	{Constructor: newHTTPHandler},
}
