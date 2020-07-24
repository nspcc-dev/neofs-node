package container

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
	"github.com/nspcc-dev/netmap"
)

// BasicACL represents the basic
// ACL rules.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/basic.ACL.
type BasicACL = basic.ACL

// PlacementRule represents placement
// rules of the container.
//
// It is a type alias of
// github.com/nspcc-dev/netmap.PlacementRule.
// FIXME: container placement rules should be defined in core lib.
type PlacementRule = netmap.PlacementRule

// Container represents NeoFS container.
type Container struct {
	basicACL BasicACL // basic ACL mask

	ownerID OwnerID // the identifier of container's owner

	salt []byte // unique container bytes

	placementRule PlacementRule // placement rules
}

// ErrNilContainer is the error returned by functions that
// expect a non-nil container pointer, but received nil.
var ErrNilContainer = errors.New("container is nil")

// OwnerID returns an ID of the container's owner.
func (c *Container) OwnerID() OwnerID {
	return c.ownerID
}

// SetOwnerID sets the ID of the container's owner.
func (c *Container) SetOwnerID(v OwnerID) {
	c.ownerID = v
}

// Salt returns the container salt.
//
// Slice is returned by reference without copying.
func (c *Container) Salt() []byte {
	return c.salt
}

// SetSalt sets the container salt.
//
// Slice is assigned by reference without copying.
func (c *Container) SetSalt(v []byte) {
	c.salt = v
}

// BasicACL returns the mask of basic container permissions.
func (c *Container) BasicACL() BasicACL {
	return c.basicACL
}

// SetBasicACL sets the mask of basic container permissions.
func (c *Container) SetBasicACL(v BasicACL) {
	c.basicACL = v
}

// PlacementRule returns placement rule of the container.
func (c *Container) PlacementRule() PlacementRule {
	return c.placementRule
}

// SetPlacementRule sets placement rule of the container.
func (c *Container) SetPlacementRule(v PlacementRule) {
	c.placementRule = v
}
