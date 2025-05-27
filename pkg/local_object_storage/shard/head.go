package shard

import (
	"context"

	"github.com/nspcc-dev/neofs-sdk-go/debugprint"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Head reads header of the object from the shard. raw flag controls split
// object handling, if unset, then virtual object header is returned, otherwise
// SplitInfo of this object.
//
// Returns any error encountered.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in Shard.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object has been marked as removed in shard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) Head(ctx context.Context, addr oid.Address, raw bool) (*objectSDK.Object, error) {
	if s.GetMode().NoMetabase() {
		st := debugprint.LogRequestStageStart(ctx, "shard GET")
		res, err := s.Get(ctx, addr, true)
		debugprint.LogRequestStageFinish(st)
		return res, err
	}
	st := debugprint.LogRequestStageStart(ctx, "metabase HEAD")
	res, err := s.metaBase.Get(ctx, addr, raw)
	debugprint.LogRequestStageFinish(st)
	return res, err
}
