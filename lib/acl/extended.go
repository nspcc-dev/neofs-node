package acl

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/nspcc-dev/neofs-api-go/refs"
)

// TypedHeaderSource is a various types of header set interface.
type TypedHeaderSource interface {
	// Must return list of Header of particular type.
	// Must return false if there is no ability to compose header list.
	HeadersOfType(acl.HeaderType) ([]acl.Header, bool)
}

// ExtendedACLSource is an interface of storage of extended ACL tables with read access.
type ExtendedACLSource interface {
	// Must return extended ACL table by container ID key.
	GetExtendedACLTable(context.Context, refs.CID) (acl.ExtendedACLTable, error)
}

// ExtendedACLStore is an interface of storage of extended ACL tables.
type ExtendedACLStore interface {
	ExtendedACLSource

	// Must store extended ACL table for container ID key.
	PutExtendedACLTable(context.Context, refs.CID, acl.ExtendedACLTable) error
}
