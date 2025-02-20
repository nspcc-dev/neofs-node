package client

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
)

// connection is a WSClient with associated actors and wrappers, it's valid as
// long as connection doesn't change.
type connection struct {
	client   *rpcclient.WSClient // neo-go websocket client
	rpcActor *actor.Actor        // neo-go RPC actor
	gasToken *nep17.Token        // neo-go GAS token wrapper
	rolemgmt *rolemgmt.Contract  // neo-go Designation contract wrapper

	// notification receivers (Client reads notifications from these channels)
	notifyChan chan *state.ContainedNotificationEvent
	headerChan chan *block.Header
	notaryChan chan *result.NotaryRequestEvent
}

func (c *connection) roleList(r noderoles.Role) (keys.PublicKeys, error) {
	height, err := c.rpcActor.GetBlockCount()
	if err != nil {
		return nil, fmt.Errorf("can't get chain height: %w", err)
	}

	return c.rolemgmt.GetDesignatedByRole(r, height)
}

// reachedHeight checks if [Client] has least expected block height and
// returns error if it is not reached that height.
// This function is required to avoid connections to unsynced RPC nodes, because
// they can produce events from the past that should not be processed by
// NeoFS nodes.
func (c *connection) reachedHeight(startFrom uint32) error {
	if startFrom == 0 {
		return nil
	}

	height, err := c.client.GetBlockCount()
	if err != nil {
		return fmt.Errorf("could not get block height: %w", err)
	}

	if height < startFrom+1 {
		return fmt.Errorf("%w: expected %d height, got %d count", ErrStaleNodes, startFrom, height)
	}

	return nil
}

func (c *connection) Close() {
	_ = c.client.UnsubscribeAll()
	c.client.Close()

	var notifyCh, headerCh, notaryCh = c.notifyChan, c.headerChan, c.notaryChan
drainloop:
	for {
		select {
		case _, ok := <-notifyCh:
			if !ok {
				notifyCh = nil
			}
		case _, ok := <-headerCh:
			if !ok {
				headerCh = nil
			}
		case _, ok := <-notaryCh:
			if !ok {
				notaryCh = nil
			}
		default:
			break drainloop
		}
	}
}
