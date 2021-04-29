package router

import (
	"bytes"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
)

var errWrongRoute = errors.New("wrong route")

// CheckRoute checks if the route is a route correctly constructed by the builder for value a.
//
// Returns nil if route is correct, otherwise an error clarifying the inconsistency.
func CheckRoute(builder Builder, epoch uint64, t reputation.Trust, route []common.ServerInfo) error {
	for i := 1; i < len(route); i++ {
		servers, err := builder.NextStage(epoch, t, route[:i])
		if err != nil {
			return err
		} else if len(servers) == 0 {
			break
		}

		found := false

		for j := range servers {
			if bytes.Equal(servers[j].PublicKey(), route[i].PublicKey()) {
				found = true
				break
			}
		}

		if !found {
			return errWrongRoute
		}
	}

	return nil
}
