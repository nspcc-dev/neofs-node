package bootstrap

import (
	"github.com/nspcc-dev/neofs-node/lib/fix/module"
)

// Module is a module of bootstrap component.
var Module = module.Module{
	{Constructor: newHealthy},
}
