package workers

import (
	"github.com/nspcc-dev/neofs-node/lib/fix/module"
)

// Module is a workers module.
var Module = module.Module{
	{Constructor: prepare},
}
