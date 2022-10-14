package object

import (
	"context"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// NodeState is storage node state processed by Object service.
type NodeState interface {
	// IsMaintenance checks if node is under maintenance. Node MUST NOT serve
	// local object operations. Node MUST respond with apistatus.NodeUnderMaintenance
	// error if IsMaintenance returns true.
	IsMaintenance() bool
}

// Common is an Object API ServiceServer which encapsulates logic spread to all
// object operations.
//
// If underlying NodeState.IsMaintenance returns true, all operations are
// immediately failed with apistatus.NodeUnderMaintenance.
type Common struct {
	state NodeState

	nextHandler ServiceServer
}

// Init initializes the Common instance.
func (x *Common) Init(state NodeState, nextHandler ServiceServer) {
	x.state = state
	x.nextHandler = nextHandler
}

var errMaintenance apistatus.NodeUnderMaintenance

func (x *Common) Get(req *objectV2.GetRequest, stream GetObjectStream) error {
	if x.state.IsMaintenance() {
		return errMaintenance
	}

	return x.nextHandler.Get(req, stream)
}

func (x *Common) Put(ctx context.Context) (PutObjectStream, error) {
	if x.state.IsMaintenance() {
		return nil, errMaintenance
	}

	return x.nextHandler.Put(ctx)
}

func (x *Common) Head(ctx context.Context, req *objectV2.HeadRequest) (*objectV2.HeadResponse, error) {
	if x.state.IsMaintenance() {
		return nil, errMaintenance
	}

	return x.nextHandler.Head(ctx, req)
}

func (x *Common) Search(req *objectV2.SearchRequest, stream SearchStream) error {
	if x.state.IsMaintenance() {
		return errMaintenance
	}

	return x.nextHandler.Search(req, stream)
}

func (x *Common) Delete(ctx context.Context, req *objectV2.DeleteRequest) (*objectV2.DeleteResponse, error) {
	if x.state.IsMaintenance() {
		return nil, errMaintenance
	}

	return x.nextHandler.Delete(ctx, req)
}

func (x *Common) GetRange(req *objectV2.GetRangeRequest, stream GetObjectRangeStream) error {
	if x.state.IsMaintenance() {
		return errMaintenance
	}

	return x.nextHandler.GetRange(req, stream)
}

func (x *Common) GetRangeHash(ctx context.Context, req *objectV2.GetRangeHashRequest) (*objectV2.GetRangeHashResponse, error) {
	if x.state.IsMaintenance() {
		return nil, errMaintenance
	}

	return x.nextHandler.GetRangeHash(ctx, req)
}
