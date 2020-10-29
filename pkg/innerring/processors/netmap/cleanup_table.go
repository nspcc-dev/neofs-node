package netmap

import (
	"encoding/hex"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
)

type (
	cleanupTable struct {
		*sync.RWMutex
		enabled    bool
		threshold  uint64
		lastAccess map[string]epochStamp
	}

	epochStamp struct {
		epoch      uint64
		removeFlag bool
	}
)

func newCleanupTable(enabled bool, threshold uint64) cleanupTable {
	return cleanupTable{
		RWMutex:    new(sync.RWMutex),
		enabled:    enabled,
		threshold:  threshold,
		lastAccess: make(map[string]epochStamp),
	}
}

// Update cleanup table based on on-chain information about netmap.
func (c *cleanupTable) update(snapshot []netmap.NodeInfo, now uint64) {
	c.Lock()
	defer c.Unlock()

	// replacing map is less memory efficient but faster
	newMap := make(map[string]epochStamp, len(snapshot))

	for i := range snapshot {
		keyString := hex.EncodeToString(snapshot[i].PublicKey)
		if access, ok := c.lastAccess[keyString]; ok {
			access.removeFlag = false // reset remove Flag on each Update
			newMap[keyString] = access
		} else {
			newMap[keyString] = epochStamp{epoch: now}
		}
	}

	c.lastAccess = newMap
}

func (c *cleanupTable) touch(keyString string, now uint64) bool {
	c.Lock()
	defer c.Unlock()

	access, ok := c.lastAccess[keyString]
	result := !access.removeFlag && ok

	access.removeFlag = false // reset remove flag on each touch
	if now > access.epoch {
		access.epoch = now
	}

	c.lastAccess[keyString] = access

	return result
}

func (c *cleanupTable) flag(keyString string) {
	c.Lock()
	defer c.Unlock()

	if access, ok := c.lastAccess[keyString]; ok {
		access.removeFlag = true
		c.lastAccess[keyString] = access
	}
}

func (c *cleanupTable) forEachRemoveCandidate(epoch uint64, f func(string) error) error {
	c.Lock()
	defer c.Unlock()

	for keyString, access := range c.lastAccess {
		if epoch-access.epoch > c.threshold {
			access.removeFlag = true // set remove flag
			c.lastAccess[keyString] = access

			if err := f(keyString); err != nil {
				return err
			}
		}
	}

	return nil
}
