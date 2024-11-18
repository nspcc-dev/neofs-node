package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type (
	// filterGroup is a structure that have search filters grouped by access
	// method. We have fast filters that looks for indexes and do not unmarshal
	// objects, and slow filters, that applied after fast filters created
	// smaller set of objects to check.
	filterGroup struct {
		withCnrFilter bool

		cnr cid.ID

		fastFilters, slowFilters object.SearchFilters
	}
)

// Select returns list of addresses of objects that match search filters.
//
// Only creation epoch, payload size, user attributes and unknown system ones
// are allowed with numeric operators. Values of numeric filters must be base-10
// integers.
//
// Returns [object.ErrInvalidSearchQuery] if specified query is invalid.
func (db *DB) Select(cnr cid.ID, filters object.SearchFilters) ([]oid.Address, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	if blindlyProcess(filters) {
		return nil, nil
	}

	var (
		addrList  []oid.Address
		currEpoch = db.epochState.CurrentEpoch()
		err       error
	)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		addrList, err = db.selectObjects(tx, cnr, filters, currEpoch)

		return err
	})
	return addrList, err
}

func (db *DB) selectObjects(tx *bbolt.Tx, cnr cid.ID, fs object.SearchFilters, currEpoch uint64) ([]oid.Address, error) {
	group, err := groupFilters(fs)
	if err != nil {
		return nil, err
	}

	// if there are conflicts in query and container then it means that there is no
	// objects to match this query.
	if group.withCnrFilter && cnr != group.cnr {
		return nil, nil
	}

	// keep matched addresses in this cache
	// value equal to number (index+1) of latest matched filter
	mAddr := make(map[string]int)

	expLen := len(group.fastFilters) // expected value of matched filters in mAddr

	if len(group.fastFilters) == 0 {
		expLen = 1

		db.selectAll(tx, cnr, mAddr)
	} else {
		for i := range group.fastFilters {
			db.selectFastFilter(tx, cnr, group.fastFilters[i], mAddr, i)
		}
	}

	res := make([]oid.Address, 0, len(mAddr))

	for a, ind := range mAddr {
		if ind != expLen {
			continue // ignore objects with unmatched fast filters
		}

		var id oid.ID
		err = id.Decode([]byte(a))
		if err != nil {
			return nil, err
		}

		var addr oid.Address
		addr.SetContainer(cnr)
		addr.SetObject(id)

		if objectStatus(tx, addr, currEpoch) > 0 {
			continue // ignore removed objects
		}

		if !db.matchSlowFilters(tx, addr, group.slowFilters, currEpoch) {
			continue // ignore objects with unmatched slow filters
		}

		res = append(res, addr)
	}

	return res, nil
}

// selectAll adds to resulting cache all available objects in metabase.
func (db *DB) selectAll(tx *bbolt.Tx, cnr cid.ID, to map[string]int) {
	bucketName := make([]byte, bucketKeySize)
	selectAllFromBucket(tx, primaryBucketName(cnr, bucketName), to, 0)
	selectAllFromBucket(tx, tombstoneBucketName(cnr, bucketName), to, 0)
	selectAllFromBucket(tx, storageGroupBucketName(cnr, bucketName), to, 0)
	selectAllFromBucket(tx, parentBucketName(cnr, bucketName), to, 0)
	selectAllFromBucket(tx, bucketNameLockers(cnr, bucketName), to, 0)
	selectAllFromBucket(tx, linkObjectsBucketName(cnr, bucketName), to, 0)
}

// selectAllFromBucket goes through all keys in bucket and adds them in a
// resulting cache. Keys should be stringed object ids.
func selectAllFromBucket(tx *bbolt.Tx, name []byte, to map[string]int, fNum int) {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return
	}

	_ = bkt.ForEach(func(k, v []byte) error {
		markAddressInCache(to, fNum, string(k))

		return nil
	})
}

// selectFastFilter makes fast optimized checks for well known buckets or
// looking through user attribute buckets otherwise.
func (db *DB) selectFastFilter(
	tx *bbolt.Tx,
	cnr cid.ID, // container we search on
	f object.SearchFilter, // fast filter
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	currEpoch := db.epochState.CurrentEpoch()
	bucketName := make([]byte, bucketKeySize)
	switch f.Header() {
	case object.FilterID:
		db.selectObjectID(tx, f, cnr, to, fNum, currEpoch)
	case object.FilterOwnerID:
		bucketName := ownerBucketName(cnr, bucketName)
		db.selectFromFKBT(tx, bucketName, f, to, fNum)
	case object.FilterPayloadChecksum:
		bucketName := payloadHashBucketName(cnr, bucketName)
		db.selectFromList(tx, bucketName, f, to, fNum)
	case object.FilterType:
		for _, bucketName := range bucketNamesForType(cnr, f.Operation(), f.Value()) {
			selectAllFromBucket(tx, bucketName, to, fNum)
		}
	case object.FilterParentID:
		bucketName := parentBucketName(cnr, bucketName)
		db.selectFromList(tx, bucketName, f, to, fNum)
	case object.FilterSplitID:
		bucketName := splitBucketName(cnr, bucketName)
		db.selectFromList(tx, bucketName, f, to, fNum)
	case object.FilterFirstSplitObject:
		bucketName := firstObjectIDBucketName(cnr, bucketName)
		db.selectFromList(tx, bucketName, f, to, fNum)
	case object.FilterRoot:
		selectAllFromBucket(tx, rootBucketName(cnr, bucketName), to, fNum)
	case object.FilterPhysical:
		selectAllFromBucket(tx, primaryBucketName(cnr, bucketName), to, fNum)
		selectAllFromBucket(tx, tombstoneBucketName(cnr, bucketName), to, fNum)
		selectAllFromBucket(tx, storageGroupBucketName(cnr, bucketName), to, fNum)
		selectAllFromBucket(tx, bucketNameLockers(cnr, bucketName), to, fNum)
		selectAllFromBucket(tx, linkObjectsBucketName(cnr, bucketName), to, fNum)
	default: // user attribute
		bucketName := attributeBucketName(cnr, f.Header(), bucketName)

		if f.Operation() == object.MatchNotPresent {
			selectOutsideFKBT(tx, allBucketNames(cnr), bucketName, to, fNum)
		} else {
			db.selectFromFKBT(tx, bucketName, f, to, fNum)
		}
	}
}

var mBucketNaming = map[string][]func(cid.ID, []byte) []byte{
	object.TypeRegular.String():      {primaryBucketName, parentBucketName},
	object.TypeTombstone.String():    {tombstoneBucketName},
	object.TypeStorageGroup.String(): {storageGroupBucketName},
	object.TypeLock.String():         {bucketNameLockers},
	object.TypeLink.String():         {linkObjectsBucketName},
}

func allBucketNames(cnr cid.ID) (names [][]byte) {
	for _, fns := range mBucketNaming {
		for _, fn := range fns {
			names = append(names, fn(cnr, make([]byte, bucketKeySize)))
		}
	}

	return
}

func bucketNamesForType(cnr cid.ID, mType object.SearchMatchType, typeVal string) (names [][]byte) {
	appendNames := func(key string) {
		fns, ok := mBucketNaming[key]
		if ok {
			for _, fn := range fns {
				names = append(names, fn(cnr, make([]byte, bucketKeySize)))
			}
		}
	}

	switch mType {
	default:
	case object.MatchStringNotEqual:
		for key := range mBucketNaming {
			if key != typeVal {
				appendNames(key)
			}
		}
	case object.MatchStringEqual:
		appendNames(typeVal)
	case object.MatchCommonPrefix:
		for key := range mBucketNaming {
			if strings.HasPrefix(key, typeVal) {
				appendNames(key)
			}
		}
	}

	return
}

// selectFromList looks into <fkbt> index to find list of addresses to add in
// resulting cache.
func (db *DB) selectFromFKBT(
	tx *bbolt.Tx,
	name []byte, // fkbt root bucket name
	f object.SearchFilter, // filter for operation and value
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	var nonNumMatcher matcher
	op := f.Operation()

	isNumOp := isNumericOp(op)
	if !isNumOp {
		var ok bool
		nonNumMatcher, ok = db.matchers[op]
		if !ok {
			db.log.Debug("missing matcher", zap.Uint32("operation", uint32(op)))
			return
		}
	}

	fkbtRoot := tx.Bucket(name)
	if fkbtRoot == nil {
		return
	}

	if isNumOp {
		// TODO: big math takes less code but inefficient
		filterNum, ok := new(big.Int).SetString(f.Value(), 10)
		if !ok {
			db.log.Debug("unexpected non-decimal numeric filter", zap.String("value", f.Value()))
			return
		}

		var objNum big.Int

		err := fkbtRoot.ForEach(func(objVal, _ []byte) error {
			if len(objVal) == 0 {
				return nil
			}

			_, ok := objNum.SetString(string(objVal), 10)
			if !ok {
				return nil
			}

			switch objNum.Cmp(filterNum) {
			case -1:
				ok = op == object.MatchNumLT || op == object.MatchNumLE
			case 0:
				ok = op == object.MatchNumLE || op == object.MatchNumGE
			case 1:
				ok = op == object.MatchNumGT || op == object.MatchNumGE
			}
			if !ok {
				return nil
			}

			fkbtLeaf := fkbtRoot.Bucket(objVal)
			if fkbtLeaf == nil {
				return nil
			}

			return fkbtLeaf.ForEach(func(objAddr, _ []byte) error {
				markAddressInCache(to, fNum, string(objAddr))
				return nil
			})
		})
		if err != nil {
			db.log.Debug("error in FKBT selection", zap.String("error", err.Error()))
		}

		return
	}

	err := nonNumMatcher.matchBucket(fkbtRoot, f.Header(), f.Value(), func(k, _ []byte) error {
		fkbtLeaf := fkbtRoot.Bucket(k)
		if fkbtLeaf == nil {
			return nil
		}

		return fkbtLeaf.ForEach(func(k, _ []byte) error {
			markAddressInCache(to, fNum, string(k))

			return nil
		})
	})
	if err != nil {
		db.log.Debug("error in FKBT selection", zap.String("error", err.Error()))
	}
}

// selectOutsideFKBT looks into all incl buckets to find list of addresses outside <fkbt> to add in
// resulting cache.
func selectOutsideFKBT(
	tx *bbolt.Tx,
	incl [][]byte, // buckets
	name []byte, // fkbt root bucket name
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	mExcl := make(map[string]struct{})

	bktExcl := tx.Bucket(name)
	if bktExcl != nil {
		_ = bktExcl.ForEach(func(k, _ []byte) error {
			exclBktLeaf := bktExcl.Bucket(k)
			if exclBktLeaf == nil {
				return nil
			}

			return exclBktLeaf.ForEach(func(k, _ []byte) error {
				mExcl[string(k)] = struct{}{}

				return nil
			})
		})
	}

	for i := range incl {
		bktIncl := tx.Bucket(incl[i])
		if bktIncl == nil {
			continue
		}

		_ = bktIncl.ForEach(func(k, _ []byte) error {
			if _, ok := mExcl[string(k)]; !ok {
				markAddressInCache(to, fNum, string(k))
			}

			return nil
		})
	}
}

// selectFromList looks into <list> index to find list of addresses to add in
// resulting cache.
func (db *DB) selectFromList(
	tx *bbolt.Tx,
	name []byte, // list root bucket name
	f object.SearchFilter, // filter for operation and value
	to map[string]int, // resulting cache
	fNum int, // index of filter
) { //
	bkt := tx.Bucket(name)
	if bkt == nil {
		return
	}

	var (
		lst [][]byte
		err error
	)

	switch op := f.Operation(); op {
	case object.MatchStringEqual:
		lst, err = decodeList(bkt.Get(bucketKeyHelper(f.Header(), f.Value())))
		if err != nil {
			db.log.Debug("can't decode list bucket leaf", zap.String("error", err.Error()))
			return
		}
	default:
		fMatch, ok := db.matchers[op]
		if !ok {
			db.log.Debug("unknown operation", zap.Uint32("operation", uint32(op)))

			return
		}

		if err = fMatch.matchBucket(bkt, f.Header(), f.Value(), func(key, val []byte) error {
			l, err := decodeList(val)
			if err != nil {
				db.log.Debug("can't decode list bucket leaf",
					zap.String("error", err.Error()),
				)

				return err
			}

			lst = append(lst, l...)

			return nil
		}); err != nil {
			db.log.Debug("can't iterate over the bucket",
				zap.String("error", err.Error()),
			)

			return
		}
	}

	for i := range lst {
		markAddressInCache(to, fNum, string(lst[i]))
	}
}

// selectObjectID processes objectID filter with in-place optimizations.
func (db *DB) selectObjectID(
	tx *bbolt.Tx,
	f object.SearchFilter,
	cnr cid.ID,
	to map[string]int, // resulting cache
	fNum int, // index of filter
	currEpoch uint64,
) {
	appendOID := func(id oid.ID) {
		var addr oid.Address
		addr.SetContainer(cnr)
		addr.SetObject(id)

		ok, err := db.exists(tx, addr, currEpoch)
		if (err == nil && ok) || errors.As(err, &splitInfoError) {
			markAddressInCache(to, fNum, string(id[:]))
		}
	}

	switch op := f.Operation(); op {
	case object.MatchStringEqual:
		var id oid.ID
		if err := id.DecodeString(f.Value()); err == nil {
			appendOID(id)
		}
	default:
		fMatch, ok := db.matchers[op]
		if !ok {
			db.log.Debug("unknown operation",
				zap.Uint32("operation", uint32(f.Operation())),
			)

			return
		}

		for _, bucketName := range bucketNamesForType(cnr, object.MatchStringNotEqual, "") {
			// copy-paste from DB.selectAllFrom
			bkt := tx.Bucket(bucketName)
			if bkt == nil {
				continue
			}

			err := fMatch.matchBucket(bkt, f.Header(), f.Value(), func(k, v []byte) error {
				var id oid.ID
				if err := id.Decode(k); err == nil {
					appendOID(id)
				}
				return nil
			})
			if err != nil {
				db.log.Debug("could not iterate over the buckets",
					zap.String("error", err.Error()),
				)
			}
		}
	}
}

// matchSlowFilters return true if object header is matched by all slow filters.
func (db *DB) matchSlowFilters(tx *bbolt.Tx, addr oid.Address, f object.SearchFilters, currEpoch uint64) bool {
	if len(f) == 0 {
		return true
	}

	buf := make([]byte, addressKeySize)
	obj, err := db.get(tx, addr, buf, true, false, currEpoch)
	if err != nil {
		return false
	}

	for i := range f {
		op := f[i].Operation()
		if isNumericOp(op) {
			attr := f[i].Header()
			if attr != object.FilterCreationEpoch && attr != object.FilterPayloadSize {
				break
			}

			// filter by creation epoch or payload size, both uint64
			filterVal := f[i].Value()
			if len(filterVal) == 0 {
				return false
			}

			if filterVal[0] == '-' {
				if op == object.MatchNumLT || op == object.MatchNumLE {
					return false
				}
				continue
			}

			if len(filterVal) > 20 { // max uint64 strlen
				if op == object.MatchNumGT || op == object.MatchNumGE {
					return false
				}
				continue
			}

			num, err := strconv.ParseUint(filterVal, 10, 64)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					if op == object.MatchNumGT || op == object.MatchNumGE {
						return false
					}
					continue
				}
				// has already been checked
				db.log.Debug("unexpected failure to parse numeric filter uint value", zap.Error(err))
				return false
			}

			var objVal uint64
			if attr == object.FilterPayloadSize {
				objVal = obj.PayloadSize()
			} else {
				objVal = obj.CreationEpoch()
			}

			switch {
			case objVal > num:
				if op == object.MatchNumLT || op == object.MatchNumLE {
					return false
				}
			case objVal == num:
				if op == object.MatchNumLT || op == object.MatchNumGT {
					return false
				}
			case objVal < num:
				if op == object.MatchNumGT || op == object.MatchNumGE {
					return false
				}
			}

			continue
		}

		matchFunc, ok := db.matchers[f[i].Operation()]
		if !ok {
			return false
		}

		var data []byte

		switch f[i].Header() {
		case object.FilterVersion:
			data = []byte(obj.Version().String())
		case object.FilterPayloadHomomorphicHash:
			cs, _ := obj.PayloadHomomorphicHash()
			data = cs.Value()
		case object.FilterCreationEpoch:
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, obj.CreationEpoch())
		case object.FilterPayloadSize:
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, obj.PayloadSize())
		default:
			continue // ignore unknown search attributes
		}

		if !matchFunc.matchSlow(f[i].Header(), data, f[i].Value()) {
			return false
		}
	}

	return true
}

// groupFilters divides filters in two groups: fast and slow. Fast filters
// processed by indexes and slow filters processed after by unmarshaling
// object headers.
func groupFilters(filters object.SearchFilters) (filterGroup, error) {
	res := filterGroup{
		fastFilters: make(object.SearchFilters, 0, len(filters)),
		slowFilters: make(object.SearchFilters, 0, len(filters)),
	}

	for i := range filters {
		hdr := filters[i].Header()
		if isNumericOp(filters[i].Operation()) {
			switch hdr {
			case
				object.FilterVersion,
				object.FilterID,
				object.FilterContainerID,
				object.FilterOwnerID,
				object.FilterPayloadChecksum,
				object.FilterType,
				object.FilterPayloadHomomorphicHash,
				object.FilterParentID,
				object.FilterSplitID,
				object.FilterRoot,
				object.FilterPhysical:
				// only object.FilterCreationEpoch and object.PayloadSize are numeric system
				// object attributes now. Unsupported system attributes will lead to an empty
				// results rather than a denial of service.
				return res, fmt.Errorf("%w: invalid filter #%d: numeric filter with non-numeric system object attribute",
					objectcore.ErrInvalidSearchQuery, i)
			}

			// TODO: big math takes less code but inefficient
			_, ok := new(big.Int).SetString(filters[i].Value(), 10)
			if !ok {
				return res, fmt.Errorf("%w: invalid filter #%d: numeric filter with non-decimal value",
					objectcore.ErrInvalidSearchQuery, i)
			}
		}

		switch hdr {
		case object.FilterContainerID: // support deprecated field
			err := res.cnr.DecodeString(filters[i].Value())
			if err != nil {
				return filterGroup{}, fmt.Errorf("can't parse container id: %w", err)
			}

			res.withCnrFilter = true
		case // slow filters
			object.FilterVersion,
			object.FilterCreationEpoch,
			object.FilterPayloadSize,
			object.FilterPayloadHomomorphicHash:
			res.slowFilters = append(res.slowFilters, filters[i])
		default: // fast filters or user attributes if unknown
			res.fastFilters = append(res.fastFilters, filters[i])
		}
	}

	return res, nil
}

func markAddressInCache(cache map[string]int, fNum int, addr string) {
	if num := cache[addr]; num == fNum {
		cache[addr] = num + 1
	}
}

// returns true if query leads to a deliberately empty result.
func blindlyProcess(fs object.SearchFilters) bool {
	for i := range fs {
		if fs[i].Operation() == object.MatchNotPresent && fs[i].IsNonAttribute() {
			return true
		}

		// TODO: #1148 check other cases
		//  e.g. (a == b) && (a != b)
	}

	return false
}

// FilterExpired filters expired object from `addresses` and return them.
// Uses internal epoch state provided via the [WithEpochState] option.
func (db *DB) FilterExpired(addresses []oid.Address) ([]oid.Address, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	epoch := db.epochState.CurrentEpoch()
	res := make([]oid.Address, 0)
	cIDToOIDs := make(map[cid.ID][]oid.ID)
	for _, a := range addresses {
		cIDToOIDs[a.Container()] = append(cIDToOIDs[a.Container()], a.Object())
	}

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		for cID, oIDs := range cIDToOIDs {
			expired, err := filterExpired(tx, epoch, cID, oIDs)
			if err != nil {
				return err
			}

			for _, oID := range expired {
				var a oid.Address
				a.SetContainer(cID)
				a.SetObject(oID)

				res = append(res, a)
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func filterExpired(tx *bbolt.Tx, epoch uint64, cID cid.ID, oIDs []oid.ID) ([]oid.ID, error) {
	objKey := make([]byte, objectKeySize)
	expAttr := object.AttributeExpirationEpoch
	expirationBucketKey := make([]byte, bucketKeySize+len(expAttr))

	res := make([]oid.ID, 0)
	notHandled := util.SliceToMap(oIDs)
	expirationBucket := tx.Bucket(attributeBucketName(cID, expAttr, expirationBucketKey))
	if expirationBucket == nil {
		return nil, nil
	}

	err := expirationBucket.ForEach(func(expBktKey, _ []byte) error {
		exp, err := strconv.ParseUint(string(expBktKey), 10, 64)
		if err != nil {
			return fmt.Errorf("could not parse expiration epoch: %w", err)
		} else if exp >= epoch {
			return nil
		}

		epochExpirationBucket := expirationBucket.Bucket(expBktKey)
		if epochExpirationBucket == nil {
			return nil
		}

		for oID := range notHandled {
			key := objectKey(oID, objKey)
			if epochExpirationBucket.Get(key) != nil {
				delete(notHandled, oID)
				res = append(res, oID)
			}
		}

		if len(notHandled) == 0 {
			return errBreakBucketForEach
		}

		return nil
	})
	if err != nil && !errors.Is(err, errBreakBucketForEach) {
		return nil, err
	}

	return res, nil
}

func isNumericOp(op object.SearchMatchType) bool {
	return op == object.MatchNumGT || op == object.MatchNumGE || op == object.MatchNumLT || op == object.MatchNumLE
}
