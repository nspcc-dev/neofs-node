package netmap

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
)

type (
	cleanupTable struct {
		*sync.RWMutex
		enabled    bool
		threshold  uint64
		lastAccess map[string]epochStampWithNodeInfo
	}

	epochStamp struct {
		epoch      uint64
		removeFlag bool
	}

	epochStampWithNodeInfo struct {
		epochStamp

		binNodeInfo []byte
	}
)

func newCleanupTable(enabled bool, threshold uint64) cleanupTable {
	return cleanupTable{
		RWMutex:    new(sync.RWMutex),
		enabled:    enabled,
		threshold:  threshold,
		lastAccess: make(map[string]epochStampWithNodeInfo),
	}
}

// Update cleanup table based on on-chain information about netmap.
func (c *cleanupTable) update(snapshot *netmap.Netmap, now uint64) {
	c.Lock()
	defer c.Unlock()

	// replacing map is less memory efficient but faster
	newMap := make(map[string]epochStampWithNodeInfo, len(snapshot.Nodes))

	for i := range snapshot.Nodes {
		binNodeInfo, err := snapshot.Nodes[i].Marshal()
		if err != nil {
			panic(fmt.Errorf("could not marshal node info: %w", err)) // seems better than ignore
		}

		keyString := hex.EncodeToString(snapshot.Nodes[i].PublicKey())

		access, ok := c.lastAccess[keyString]
		if ok {
			access.removeFlag = false // reset remove Flag on each Update
		} else {
			access.epoch = now
		}

		access.binNodeInfo = binNodeInfo

		newMap[keyString] = access
	}

	c.lastAccess = newMap
}

// updates last access time of the netmap node by string public key.
//
// Returns true if at least one condition is met:
//  * node hasn't been accessed yet;
//  * remove flag is set;
//  * binary node info has changed.
func (c *cleanupTable) touch(keyString string, now uint64, binNodeInfo []byte) bool {
	c.Lock()
	defer c.Unlock()

	access, ok := c.lastAccess[keyString]
	result := !ok || access.removeFlag || !bytes.Equal(access.binNodeInfo, binNodeInfo)

	access.removeFlag = false // reset remove flag on each touch
	if now > access.epoch {
		access.epoch = now
	}

	access.binNodeInfo = binNodeInfo // update binary node info

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
