package engine

import (
	"errors"
	"fmt"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Select selects the objects from local storage that match select parameters.
//
// Returns any error encountered that did not allow to completely select the objects.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Select(cnr cid.ID, filters object.SearchFilters) ([]oid.Address, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddSearchDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	addrList := make([]oid.Address, 0)
	uniqueMap := make(map[string]struct{})

	for _, sh := range e.unsortedShards() {
		res, err := sh.Select(cnr, filters)
		if err != nil {
			if errors.Is(err, objectcore.ErrInvalidSearchQuery) {
				return addrList, err
			}
			e.reportShardError(sh, "could not select objects from shard", err)
			continue
		}

		for _, addr := range res { // save only unique values
			if _, ok := uniqueMap[addr.EncodeToString()]; !ok {
				uniqueMap[addr.EncodeToString()] = struct{}{}
				addrList = append(addrList, addr)
			}
		}
	}

	return addrList, nil
}

// Search performs Search op on all underlying shards and returns merged result.
//
// Fails instantly if executions are blocked (see [StorageEngine.BlockExecution]).
func (e *StorageEngine) Search(cnr cid.ID, fs object.SearchFilters, fInt map[int]objectcore.ParsedIntFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddSearchDuration)()
	}
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()
	if e.blockErr != nil {
		return nil, nil, e.blockErr
	}
	shs := e.unsortedShards()
	if len(shs) == 0 {
		return nil, nil, nil
	}
	items, nextCursor, err := shs[0].Search(cnr, fs, fInt, attrs, cursor, count)
	if err != nil {
		e.reportShardError(shs[0], "could not select objects from shard", err)
	}
	if len(shs) == 1 {
		return items, nextCursor, nil
	}
	shs = shs[1:]
	sets, mores := make([][]client.SearchResultItem, 1, len(shs)), make([]bool, 1, len(shs))
	sets[0], mores[0] = items, nextCursor != nil
	for i := range shs {
		if items, nextCursor, err = shs[i].Search(cnr, fs, fInt, attrs, cursor, count); err != nil {
			e.reportShardError(shs[i], "could not select objects from shard", err)
			continue
		}
		sets, mores = append(sets, items), append(mores, nextCursor != nil)
	}
	var firstAttr string
	if len(attrs) > 0 {
		firstAttr = fs[0].Header()
	}
	cmpInt := firstAttr != "" && objectcore.IsIntegerSearchOp(fs[0].Operation())
	res, more, err := objectcore.MergeSearchResults(count, firstAttr, cmpInt, sets, mores)
	if err != nil || !more {
		return res, nil, err
	}
	c, err := objectcore.CalculateCursor(fs, res[len(res)-1])
	if err != nil {
		return nil, nil, fmt.Errorf("recalculate cursor: %w", err)
	}
	// note: if the last res element is the last element of some shard, we could
	// skip cursor calculation and win a bit. At the same time, this would require
	// merging logic complication, so for now we just always do it.
	return res[:count], c, nil
}
