package engine

import (
	"fmt"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// TODO:: docs.
func (e *StorageEngine) GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (objectSDK.Object, error) {
	// TODO: keep in sync with https://github.com/nspcc-dev/neofs-node/pull/3466.
	// TODO: metrics and blockErr like Get

	// TODO: sync placement with PUT. They must sort shard equally
	shs := e.sortedShards(oid.NewAddress(cnr, parent))
	for i := range shs {
		obj, err := shs[i].GetECPart(cnr, parent, pi)
		if err == nil {
			return obj, nil
		}
		// TODO: debug if 404
		e.log.Info("failed to get EC part from shard", zap.Stringer("shardID", shs[i].ID()), zap.Stringer("container", cnr), zap.Stringer("parent", parent),
			zap.Int("ruleIdx", pi.RuleIndex), zap.Int("partIdx", pi.Index), zap.Error(err))
		// FIXME: some errors like 'akready removed' must abort
	}

	return objectSDK.Object{}, fmt.Errorf("%w: all shards failed", apistatus.ErrObjectNotFound)
}
