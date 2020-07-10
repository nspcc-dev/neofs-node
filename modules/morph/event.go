package morph

import (
	"github.com/nspcc-dev/neofs-node/lib/blockchain/event"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/event/netmap"
)

const eventOpt = "event"

// NewEpochEventType is a config section of new epoch notification event.
const NewEpochEventType = "new_epoch"

// ContractEventOptPath returns the config path to notification event name of particular contract.
func ContractEventOptPath(contract, event string) string {
	return optPath(prefix, contract, eventOpt, event)
}

var mParsers = map[string][]struct {
	typ    string
	parser event.Parser
}{
	NetmapContractName: {
		{
			typ:    NewEpochEventType,
			parser: netmap.ParseNewEpoch,
		},
	},
}
