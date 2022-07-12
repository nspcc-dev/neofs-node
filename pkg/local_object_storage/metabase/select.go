package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
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

// SelectPrm groups the parameters of Select operation.
type SelectPrm struct {
	cnr     cid.ID
	filters object.SearchFilters
}

// SelectRes groups the resulting values of Select operation.
type SelectRes struct {
	addrList []oid.Address
}

// WithContainerID is a Select option to set the container id to search in.
func (p *SelectPrm) WithContainerID(cnr cid.ID) {
	if p != nil {
		p.cnr = cnr
	}
}

// WithFilters is a Select option to set the object filters.
func (p *SelectPrm) WithFilters(fs object.SearchFilters) {
	if p != nil {
		p.filters = fs
	}
}

// AddressList returns list of addresses of the selected objects.
func (r SelectRes) AddressList() []oid.Address {
	return r.addrList
}

// Select returns list of addresses of objects that match search filters.
func (db *DB) Select(prm SelectPrm) (res SelectRes, err error) {
	if blindlyProcess(prm.filters) {
		return res, nil
	}

	return res, db.boltDB.View(func(tx *bbolt.Tx) error {
		res.addrList, err = db.selectObjects(tx, prm.cnr, prm.filters)

		return err
	})
}

func (db *DB) selectObjects(tx *bbolt.Tx, cnr cid.ID, fs object.SearchFilters) ([]oid.Address, error) {
	group, err := groupFilters(fs)
	if err != nil {
		return nil, err
	}

	// if there are conflicts in query and container then it means that there is no
	// objects to match this query.
	if group.withCnrFilter && !cnr.Equals(group.cnr) {
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

		var addr oid.Address

		err = decodeAddressFromKey(&addr, []byte(a))
		if err != nil {
			return nil, err
		}

		if inGraveyard(tx, addr) > 0 {
			continue // ignore removed objects
		}

		if !db.matchSlowFilters(tx, addr, group.slowFilters) {
			continue // ignore objects with unmatched slow filters
		}

		res = append(res, addr)
	}

	return res, nil
}

// selectAll adds to resulting cache all available objects in metabase.
func (db *DB) selectAll(tx *bbolt.Tx, cnr cid.ID, to map[string]int) {
	prefix := cnr.EncodeToString() + "/"

	selectAllFromBucket(tx, primaryBucketName(cnr), prefix, to, 0)
	selectAllFromBucket(tx, tombstoneBucketName(cnr), prefix, to, 0)
	selectAllFromBucket(tx, storageGroupBucketName(cnr), prefix, to, 0)
	selectAllFromBucket(tx, parentBucketName(cnr), prefix, to, 0)
	selectAllFromBucket(tx, bucketNameLockers(cnr), prefix, to, 0)
}

// selectAllFromBucket goes through all keys in bucket and adds them in a
// resulting cache. Keys should be stringed object ids.
func selectAllFromBucket(tx *bbolt.Tx, name []byte, prefix string, to map[string]int, fNum int) {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return
	}

	_ = bkt.ForEach(func(k, v []byte) error {
		key := prefix + string(k) // consider using string builders from sync.Pool
		markAddressInCache(to, fNum, key)

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
	prefix := cnr.EncodeToString() + "/"

	switch f.Header() {
	case v2object.FilterHeaderObjectID:
		db.selectObjectID(tx, f, cnr, to, fNum)
	case v2object.FilterHeaderOwnerID:
		bucketName := ownerBucketName(cnr)
		db.selectFromFKBT(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderPayloadHash:
		bucketName := payloadHashBucketName(cnr)
		db.selectFromList(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderObjectType:
		for _, bucketName := range bucketNamesForType(cnr, f.Operation(), f.Value()) {
			selectAllFromBucket(tx, bucketName, prefix, to, fNum)
		}
	case v2object.FilterHeaderParent:
		bucketName := parentBucketName(cnr)
		db.selectFromList(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderSplitID:
		bucketName := splitBucketName(cnr)
		db.selectFromList(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterPropertyRoot:
		selectAllFromBucket(tx, rootBucketName(cnr), prefix, to, fNum)
	case v2object.FilterPropertyPhy:
		selectAllFromBucket(tx, primaryBucketName(cnr), prefix, to, fNum)
		selectAllFromBucket(tx, tombstoneBucketName(cnr), prefix, to, fNum)
		selectAllFromBucket(tx, storageGroupBucketName(cnr), prefix, to, fNum)
		selectAllFromBucket(tx, bucketNameLockers(cnr), prefix, to, fNum)
	default: // user attribute
		bucketName := attributeBucketName(cnr, f.Header())

		if f.Operation() == object.MatchNotPresent {
			selectOutsideFKBT(tx, allBucketNames(cnr), bucketName, f, prefix, to, fNum)
		} else {
			db.selectFromFKBT(tx, bucketName, f, prefix, to, fNum)
		}
	}
}

var mBucketNaming = map[string][]func(cid.ID) []byte{
	v2object.TypeRegular.String():      {primaryBucketName, parentBucketName},
	v2object.TypeTombstone.String():    {tombstoneBucketName},
	v2object.TypeStorageGroup.String(): {storageGroupBucketName},
	v2object.TypeLock.String():         {bucketNameLockers},
}

func allBucketNames(cnr cid.ID) (names [][]byte) {
	for _, fns := range mBucketNaming {
		for _, fn := range fns {
			names = append(names, fn(cnr))
		}
	}

	return
}

func bucketNamesForType(cnr cid.ID, mType object.SearchMatchType, typeVal string) (names [][]byte) {
	appendNames := func(key string) {
		fns, ok := mBucketNaming[key]
		if ok {
			for _, fn := range fns {
				names = append(names, fn(cnr))
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
	prefix string, // prefix to create addr from oid in index
	to map[string]int, // resulting cache
	fNum int, // index of filter
) { //
	matchFunc, ok := db.matchers[f.Operation()]
	if !ok {
		db.log.Debug("missing matcher", zap.Uint32("operation", uint32(f.Operation())))

		return
	}

	fkbtRoot := tx.Bucket(name)
	if fkbtRoot == nil {
		return
	}

	err := matchFunc.matchBucket(fkbtRoot, f.Header(), f.Value(), func(k, _ []byte) error {
		fkbtLeaf := fkbtRoot.Bucket(k)
		if fkbtLeaf == nil {
			return nil
		}

		return fkbtLeaf.ForEach(func(k, _ []byte) error {
			addr := prefix + string(k)
			markAddressInCache(to, fNum, addr)

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
	f object.SearchFilter, // filter for operation and value
	prefix string, // prefix to create addr from oid in index
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
				addr := prefix + string(k)
				mExcl[addr] = struct{}{}

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
			addr := prefix + string(k)

			if _, ok := mExcl[addr]; !ok {
				markAddressInCache(to, fNum, addr)
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
	prefix string, // prefix to create addr from oid in index
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
		addr := prefix + string(lst[i])
		markAddressInCache(to, fNum, addr)
	}
}

// selectObjectID processes objectID filter with in-place optimizations.
func (db *DB) selectObjectID(
	tx *bbolt.Tx,
	f object.SearchFilter,
	cnr cid.ID,
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	prefix := cnr.EncodeToString() + "/"

	appendOID := func(strObj string) {
		addrStr := prefix + strObj
		var addr oid.Address

		err := decodeAddressFromKey(&addr, []byte(addrStr))
		if err != nil {
			db.log.Debug("can't decode object id address",
				zap.String("addr", addrStr),
				zap.String("error", err.Error()))

			return
		}

		ok, err := db.exists(tx, addr)
		if (err == nil && ok) || errors.As(err, &splitInfoError) {
			markAddressInCache(to, fNum, addrStr)
		}
	}

	switch op := f.Operation(); op {
	case object.MatchStringEqual:
		appendOID(f.Value())
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
				return
			}

			err := fMatch.matchBucket(bkt, f.Header(), f.Value(), func(k, v []byte) error {
				appendOID(string(k))
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
func (db *DB) matchSlowFilters(tx *bbolt.Tx, addr oid.Address, f object.SearchFilters) bool {
	if len(f) == 0 {
		return true
	}

	obj, err := db.get(tx, addr, true, false)
	if err != nil {
		return false
	}

	for i := range f {
		matchFunc, ok := db.matchers[f[i].Operation()]
		if !ok {
			return false
		}

		var data []byte

		switch f[i].Header() {
		case v2object.FilterHeaderVersion:
			data = []byte(obj.Version().String())
		case v2object.FilterHeaderHomomorphicHash:
			cs, _ := obj.PayloadHomomorphicHash()
			data = cs.Value()
		case v2object.FilterHeaderCreationEpoch:
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, obj.CreationEpoch())
		case v2object.FilterHeaderPayloadLength:
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
		switch filters[i].Header() {
		case v2object.FilterHeaderContainerID: // support deprecated field
			err := res.cnr.DecodeString(filters[i].Value())
			if err != nil {
				return filterGroup{}, fmt.Errorf("can't parse container id: %w", err)
			}

			res.withCnrFilter = true
		case // slow filters
			v2object.FilterHeaderVersion,
			v2object.FilterHeaderCreationEpoch,
			v2object.FilterHeaderPayloadLength,
			v2object.FilterHeaderHomomorphicHash:
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
		if fs[i].Operation() == object.MatchNotPresent && isSystemKey(fs[i].Header()) {
			return true
		}

		// TODO: #1148 check other cases
		//  e.g. (a == b) && (a != b)
	}

	return false
}

// returns true if string key is a reserved system filter key.
func isSystemKey(key string) bool {
	// FIXME: #1147 version-dependent approach
	return strings.HasPrefix(key, v2object.ReservedFilterPrefix)
}
