package morph

import (
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/module"
)

// Module is a Neo:Morph module.
var Module = module.Module{
	{Constructor: newClient},
	{Constructor: newMorphContracts},
	{Constructor: newContainerContract},
	{Constructor: newNetmapContract},
	{Constructor: newEventListener},
	{Constructor: newBalanceContract},
}

func optPath(sections ...string) string {
	return strings.Join(sections, ".")
}
