package eacl

import (
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
)

// Source is the interface that wraps
// basic methods of extended ACL table source.
type Source interface {
	// GetEACL reads the table from the source by identifier.
	// It returns any error encountered.
	//
	// GetEACL must return exactly one non-nil value.
	//
	// Must return pkg/core/container.ErrEACLNotFound if requested
	// eACL table is not in source.
	GetEACL(cid.ID) (*eacl.Table, error)
}
