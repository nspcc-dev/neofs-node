package settings

import (
	"github.com/nspcc-dev/neofs-node/lib/fix/module"
)

// Module is a node settings module.
var Module = module.Module{
	{Constructor: newNodeSettings},
}
