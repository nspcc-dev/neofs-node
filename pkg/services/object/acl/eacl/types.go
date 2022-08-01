package eacl

import (
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// Source is the interface that wraps
// basic methods of extended ACL table source.
type Source interface {
	// GetEACL reads the table from the source by identifier.
	// It returns any error encountered.
	//
	// GetEACL must return exactly one non-nil value.
	//
	// Must return apistatus.ErrEACLNotFound if requested
	// eACL table is not in source.
	GetEACL(cid.ID) (*containercore.EACL, error)
}
