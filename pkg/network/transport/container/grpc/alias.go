package container

import (
	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
)

// CID represents the container identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.ID.
type CID = container.ID

// OwnerID represents the container owner identifier..
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.OwnerID.
type OwnerID = container.OwnerID

// Container represents the NeoFS Container structure.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.Container.
type Container = container.Container

// Table represents the eACL table.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/acl/extended.ExtendedACLTable.
type Table = eacl.Table
