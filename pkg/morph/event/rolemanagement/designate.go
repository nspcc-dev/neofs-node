package rolemanagement

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// Designate represents designation event of the mainnet RoleManagement contract.
type Designate struct {
	Role noderoles.Role

	// TxHash is used in notary environmental
	// for calculating unique but same for
	// all notification receivers values.
	TxHash util.Uint256
}

// MorphEvent implements Neo:Morph Event interface.
func (Designate) MorphEvent() {}

// ParseDesignate from notification into container event structure.
func ParseDesignate(e *subscriptions.NotificationEvent) (event.Event, error) {
	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	if len(params) != 2 {
		return nil, event.WrongNumberOfParameters(2, len(params))
	}

	bi, err := params[0].TryInteger()
	if err != nil {
		return nil, fmt.Errorf("invalid stackitem type: %w", err)
	}

	return Designate{
		Role:   noderoles.Role(bi.Int64()),
		TxHash: e.Container,
	}, nil
}
