package managers

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationroute "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/route"
)

// ManagerBuilder defines an interface for providing a list
// of Managers for specific epoch. Implementation depends on trust value.
type ManagerBuilder interface {
	// BuildManagers must compose list of managers. It depends on
	// particular epoch and PeerID of the current route point.
	BuildManagers(epoch uint64, p reputation.PeerID) ([]reputationroute.ServerInfo, error)
}
