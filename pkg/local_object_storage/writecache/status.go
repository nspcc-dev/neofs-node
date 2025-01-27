package writecache

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectStatus represents the status of the object in the Writecache.
type ObjectStatus struct {
	PathFSTree string
}

// ObjectStatus returns the status of the object in the Writecache. It contains path to the FSTree.
func (c *cache) ObjectStatus(address oid.Address) (ObjectStatus, error) {
	var res ObjectStatus

	_, err := c.fsTree.Get(address)
	if err == nil {
		res.PathFSTree = c.fsTree.Path()
	}
	return res, err
}
