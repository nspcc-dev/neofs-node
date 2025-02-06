package netmap

import (
	"bytes"
	"slices"
	"sync"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

type (
	cleanupTable struct {
		*sync.RWMutex
		enabled    bool
		threshold  uint64
		lastAccess map[string]epochStampWithNodeInfo

		prev netmap.NetMap
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

// Update cleanup table based on on-chain information about netmap. Returned
// value indicates if the composition of network map memebers has changed.
func (c *cleanupTable) update(snapshot netmap.NetMap, now uint64) bool {
	c.Lock()
	defer c.Unlock()

	nmNodes := snapshot.Nodes()

	// replacing map is less memory efficient but faster
	newMap := make(map[string]epochStampWithNodeInfo, len(nmNodes))

	for i := range nmNodes {
		binNodeInfo := nmNodes[i].Marshal()

		keyString := netmap.StringifyPublicKey(nmNodes[i])

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

	// order is expected to be the same from epoch to epoch
	mapChanged := !slices.EqualFunc(c.prev.Nodes(), nmNodes, func(i1 netmap.NodeInfo, i2 netmap.NodeInfo) bool {
		return bytes.Equal(i1.PublicKey(), i2.PublicKey())
	})
	c.prev = snapshot

	return mapChanged
}

// updates last access time of the netmap node by string public key.
//
// Returns true if at least one condition is met:
//   - node hasn't been accessed yet;
//   - remove flag is set;
//   - binary node info has changed.
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

func (c *cleanupTable) flag(keyString string, now uint64) {
	c.Lock()
	defer c.Unlock()

	if access, ok := c.lastAccess[keyString]; ok {
		access.removeFlag = true
		access.epoch = now
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
