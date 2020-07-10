package node

import (
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/event"
	"github.com/nspcc-dev/neofs-node/lib/boot"
	"github.com/nspcc-dev/neofs-node/lib/fix/module"
	"github.com/nspcc-dev/neofs-node/lib/fix/worker"
	"github.com/nspcc-dev/neofs-node/lib/metrics"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/nspcc-dev/neofs-node/lib/replication"
	"github.com/nspcc-dev/neofs-node/modules/bootstrap"
	"github.com/nspcc-dev/neofs-node/modules/grpc"
	"github.com/nspcc-dev/neofs-node/modules/morph"
	"github.com/nspcc-dev/neofs-node/modules/network"
	"github.com/nspcc-dev/neofs-node/modules/settings"
	"github.com/nspcc-dev/neofs-node/modules/workers"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type jobParams struct {
	dig.In

	Logger *zap.Logger
	Viper  *viper.Viper
	Peers  peers.Store

	Replicator     replication.Manager
	PeersInterface peers.Interface
	Metrics        metrics.Collector

	MorphEventListener event.Listener

	StorageBootController boot.StorageBootController
}

// Module is a NeoFS node module.
var Module = module.Module{
	{Constructor: attachJobs},
	{Constructor: newPeerstore},
	{Constructor: attachServices},
	{Constructor: netmap.NewNetmap},
	{Constructor: newStorage},
	{Constructor: newMetricsCollector},
	{Constructor: newObjectCounter},

	// -- Container gRPC handlers -- //
	{Constructor: newContainerService},

	// -- gRPC Services -- //

	// -- Local store -- //
	{Constructor: newLocalstore},

	// -- Object manager -- //
	{Constructor: newObjectManager},

	// -- Replication manager -- //
	{Constructor: newReplicationManager},

	// -- Session service -- //
	{Constructor: session.NewMapTokenStore},
	{Constructor: newSessionService},

	// -- Placement tool -- //
	{Constructor: newPlacementTool},

	// metrics service -- //
	{Constructor: newMetricsService},
}.Append(
	// app specific modules:
	grpc.Module,
	network.Module,
	workers.Module,
	settings.Module,
	bootstrap.Module,
	morph.Module,
)

func attachJobs(p jobParams) worker.Jobs {
	return worker.Jobs{
		"peers":          p.PeersInterface.Job,
		"metrics":        p.Metrics.Start,
		"event_listener": p.MorphEventListener.Listen,
		"replicator":     p.Replicator.Process,
		"boot":           p.StorageBootController.Bootstrap,
	}
}
