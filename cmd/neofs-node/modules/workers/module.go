package workers

import "github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/module"

// Module is a workers module.
var Module = module.Module{
	{Constructor: prepare},
}
