package morph

import (
	"strings"

	"github.com/nspcc-dev/neofs-node/lib/fix/module"
)

// Module is a Neo:Morph module.
var Module = module.Module{
	{Constructor: newMorphClient},
	{Constructor: newMorphContracts},
	{Constructor: newContainerContract},
	{Constructor: newReputationContract},
	{Constructor: newNetmapContract},
	{Constructor: newEventListener},
	{Constructor: newBalanceContract},
}

func optPath(sections ...string) string {
	return strings.Join(sections, ".")
}
