package grpc

import "github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/module"

// Module is a gRPC layer module.
var Module = module.Module{
	{Constructor: routing},
}
