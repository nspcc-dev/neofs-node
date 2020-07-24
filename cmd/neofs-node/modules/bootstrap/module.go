package bootstrap

import "github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/module"

// Module is a module of bootstrap component.
var Module = module.Module{
	{Constructor: newHealthy},
}
