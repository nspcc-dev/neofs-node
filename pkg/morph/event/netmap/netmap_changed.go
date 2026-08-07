package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// NetmapChanged contains addNode method parameters.
type NetmapChanged netmaprpc.NewNetmapEvent

// NetmapVersion returns new network map version number.
func (n NetmapChanged) NetmapVersion() int {
	return int(n.Version.Int64())
}

// MorphEvent implements Neo:Morph Event interface.
func (NetmapChanged) MorphEvent() {}

// ParseNewNetmapVersion is a parser of new netmap version notification event.
//
// The result is of NetmapChanged type.
func ParseNewNetmapVersion(e *state.ContainedNotificationEvent) (event.Event, error) {
	var ev netmaprpc.NewNetmapEvent
	err := ev.FromStackItem(e.Item)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new netmap version event: %w", err)
	}

	return NetmapChanged(ev), nil
}
