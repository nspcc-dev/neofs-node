package balance

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	balancerpc "github.com/nspcc-dev/neofs-contract/rpc/balance"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// ChangePaymentStatus structure of container.ChangePaymentStatus notification from FS chain.
type ChangePaymentStatus struct {
	ContainerID util.Uint256
	Epoch       uint64
	Unpaid      bool
}

// MorphEvent implements Neo:Morph Event interface.
func (ChangePaymentStatus) MorphEvent() {}

// ParseChangePaymentStatus from notification into [ChangePaymentStatus] structure.
func ParseChangePaymentStatus(e *state.ContainedNotificationEvent) (event.Event, error) {
	var (
		ev    ChangePaymentStatus
		rpcEv balancerpc.ChangePaymentStatusEvent
	)

	err := rpcEv.FromStackItem(e.Item)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}
	if rpcEv.Epoch.Sign() <= 0 {
		return nil, fmt.Errorf("negative epoch")
	}

	ev.Unpaid = rpcEv.Unpaid
	ev.ContainerID = rpcEv.ContainerID
	ev.Epoch = rpcEv.Epoch.Uint64()

	return ev, nil
}
