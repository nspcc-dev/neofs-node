package container

import (
	"context"
)

// GetParams is a group of parameters for container receiving operation.
type GetParams struct {
	ctxValue

	cidValue
}

// GetResult is a group of values returned by container receiving operation.
type GetResult struct {
	cnrValue
}

// PutParams is a group of parameters for container storing operation.
type PutParams struct {
	ctxValue

	cnrValue
}

// PutResult is a group of values returned by container storing operation.
type PutResult struct {
	cidValue
}

// DeleteParams is a group of parameters for container removal operation.
type DeleteParams struct {
	ctxValue

	cidValue

	ownerID OwnerID
}

// DeleteResult is a group of values returned by container removal operation.
type DeleteResult struct{}

// ListParams is a group of parameters for container listing operation.
type ListParams struct {
	ctxValue

	ownerIDList []OwnerID
}

// ListResult is a group of values returned by container listing operation.
type ListResult struct {
	cidList []CID
}

type cnrValue struct {
	cnr *Container
}

type cidValue struct {
	cid CID
}

type ctxValue struct {
	ctx context.Context
}

// Storage is an interface of the storage of NeoFS containers.
type Storage interface {
	GetContainer(GetParams) (*GetResult, error)
	PutContainer(PutParams) (*PutResult, error)
	DeleteContainer(DeleteParams) (*DeleteResult, error)
	ListContainers(ListParams) (*ListResult, error)
	// TODO: add EACL methods
}

// Context is a context getter.
func (s ctxValue) Context() context.Context {
	return s.ctx
}

// SetContext is a context setter.
func (s *ctxValue) SetContext(v context.Context) {
	s.ctx = v
}

// CID is a container ID getter.
func (s cidValue) CID() CID {
	return s.cid
}

// SetCID is a container ID getter.
func (s *cidValue) SetCID(v CID) {
	s.cid = v
}

// Container is a container getter.
func (s cnrValue) Container() *Container {
	return s.cnr
}

// SetContainer is a container setter.
func (s *cnrValue) SetContainer(v *Container) {
	s.cnr = v
}

// OwnerID is an owner ID getter.
func (s DeleteParams) OwnerID() OwnerID {
	return s.ownerID
}

// SetOwnerID is an owner ID setter.
func (s *DeleteParams) SetOwnerID(v OwnerID) {
	s.ownerID = v
}

// OwnerIDList is an owner ID list getter.
func (s ListParams) OwnerIDList() []OwnerID {
	return s.ownerIDList
}

// SetOwnerIDList is an owner ID list setter.
func (s *ListParams) SetOwnerIDList(v ...OwnerID) {
	s.ownerIDList = v
}

// CIDList is a container ID list getter.
func (s ListResult) CIDList() []CID {
	return s.cidList
}

// SetCIDList is a container ID list setter.
func (s *ListResult) SetCIDList(v []CID) {
	s.cidList = v
}
