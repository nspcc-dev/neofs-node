package settings

import "github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/module"

// Module is a node settings module.
var Module = module.Module{
	{Constructor: newNodeSettings},
}
