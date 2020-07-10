package grpc

import (
	"github.com/nspcc-dev/neofs-node/lib/fix/module"
)

// Module is a gRPC layer module.
var Module = module.Module{
	{Constructor: routing},
}
