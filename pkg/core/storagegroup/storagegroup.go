package storagegroup

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
)

// GetSGPrm groups parameter of GetSG operation.
type GetSGPrm struct {
	Context context.Context

	OID oid.ID
	CID cid.ID

	NetMap    netmap.NetMap
	Container [][]netmap.NodeInfo
}

// SGSource is a storage group information source interface.
type SGSource interface {
	// ListSG must list container's storage group objects not expired at the
	// specified current epoch.
	//
	// Must return any error encountered which did not allow to form the list.
	ListSG(context.Context, client.NodeInfo, cid.ID, uint64) ([]oid.ID, error)

	// GetSG must return storage group object for the provided CID, OID,
	// container and netmap state.
	GetSG(GetSGPrm) (*storagegroup.StorageGroup, error)
}

// StorageGroup combines storage group object ID and its structure.
type StorageGroup struct {
	id oid.ID
	sg storagegroup.StorageGroup
}

// ID returns object ID of the storage group.
func (s StorageGroup) ID() oid.ID {
	return s.id
}

// SetID sets an object ID of the storage group.
func (s *StorageGroup) SetID(id oid.ID) {
	s.id = id
}

// StorageGroup returns the storage group descriptor.
func (s StorageGroup) StorageGroup() storagegroup.StorageGroup {
	return s.sg
}

// SetStorageGroup sets a storage group descriptor.
func (s *StorageGroup) SetStorageGroup(sg storagegroup.StorageGroup) {
	s.sg = sg
}
