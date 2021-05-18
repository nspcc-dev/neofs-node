package rolemanagement

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// Designate represents designation event of the mainnet RoleManagement contract.
type Designate struct {
	Role noderoles.Role
}

// MorphEvent implements Neo:Morph Event interface.
func (Designate) MorphEvent() {}

// ParseDesignate from notification into container event structure.
func ParseDesignate(params []stackitem.Item) (event.Event, error) {
	if len(params) != 2 {
		return nil, event.WrongNumberOfParameters(2, len(params))
	}

	bi, err := params[0].TryInteger()
	if err != nil {
		return nil, fmt.Errorf("invalid stackitem type: %w", err)
	}

	return Designate{Role: noderoles.Role(bi.Int64())}, nil
}
