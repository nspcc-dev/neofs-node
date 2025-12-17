package engine

import (
	"bytes"
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
	uniqueMap := make(map[oid.Address]struct{})

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
			if _, ok := uniqueMap[addr]; !ok {
				uniqueMap[addr] = struct{}{}
				addrList = append(addrList, addr)
			}
		}
	}

	return addrList, nil
}

// Search performs Search op on all underlying shards and returns merged result.
//
// Fails instantly if executions are blocked (see [StorageEngine.BlockExecution]).
func (e *StorageEngine) Search(cnr cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
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
	items, nextCursor, err := shs[0].Search(cnr, fs, attrs, cursor, count)
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
		if items, nextCursor, err = shs[i].Search(cnr, fs, attrs, cursor, count); err != nil {
			e.reportShardError(shs[i], "could not select objects from shard", err)
			continue
		}
		sets, mores = append(sets, items), append(mores, nextCursor != nil)
	}
	var (
		firstAttr   string
		firstFilter *object.SearchFilter
	)

	if len(attrs) > 0 {
		firstAttr = fs[0].Header()
		firstFilter = &fs[0].SearchFilter
	}
	cmpInt := firstAttr != "" && objectcore.IsIntegerSearchOp(fs[0].Operation())
	res, more, err := objectcore.MergeSearchResults(count, firstAttr, cmpInt, sets, mores)
	if err != nil || !more {
		return res, nil, err
	}
	c, err := objectcore.CalculateCursor(firstFilter, res[len(res)-1])
	if err != nil {
		return nil, nil, fmt.Errorf("recalculate cursor: %w", err)
	}
	// note: if the last res element is the last element of some shard, we could
	// skip cursor calculation and win a bit. At the same time, this would require
	// merging logic complication, so for now we just always do it.
	return res[:count], c, nil
}

func (e *StorageEngine) collectRawWithAttribute(cnr cid.ID, attr string, val []byte) ([]oid.Address, error) {
	var (
		err    error
		shards = e.unsortedShards()
		ids    = make([][]oid.ID, len(shards))
	)

	for i, sh := range shards {
		ids[i], err = sh.CollectRawWithAttribute(cnr, attr, val)
		if err != nil {
			return nil, fmt.Errorf("shard %s: %w", sh.ID(), err)
		}
	}
	return mergeOIDs(cnr, ids), nil
}

// mergeOIDs merges given set of lists of object IDs into a single flat
// list of addresses in the same given container. ids are expected to be
// sorted and the result contains no duplicates from different original
// list (lists are expected to not contain any inner duplicates).
func mergeOIDs(cnr cid.ID, ids [][]oid.ID) []oid.Address {
	var numOfRes int

	for i := range ids {
		numOfRes += len(ids[i])
	}

	if numOfRes == 0 {
		return nil
	}

	var (
		idx = make([]int, len(ids))
		res = make([]oid.Address, 0, numOfRes)
	)

	var haveUnhandledID = func() bool {
		for i := range ids {
			if idx[i] < len(ids[i]) {
				return true
			}
		}
		return false
	}

	// ids are naturally sorted by ID, rely on that.
	for haveUnhandledID() {
		var (
			minIdx = -1
			minID  oid.ID
		)

		for i := range ids {
			if idx[i] < len(ids[i]) {
				var id = ids[i][idx[i]]
				if minIdx == -1 {
					minIdx = i
					minID = id
					continue
				}
				switch bytes.Compare(minID[:], id[:]) {
				case 0:
					idx[i]++ // Skip duplicating value.
				case 1:
					minIdx = i
					minID = id
				}
			}
		}
		res = append(res, oid.NewAddress(cnr, minID))
		idx[minIdx]++
	}

	return res
}
