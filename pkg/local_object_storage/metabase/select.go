package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"go.uber.org/zap"
)

type (
	// filterGroup is a structure that have search filters grouped by access
	// method. We have fast filters that looks for indexes and do not unmarshal
	// objects, and slow filters, that applied after fast filters created
	// smaller set of objects to check.
	filterGroup struct {
		cid         *cid.ID
		fastFilters object.SearchFilters
		slowFilters object.SearchFilters
	}
)

// SelectPrm groups the parameters of Select operation.
type SelectPrm struct {
	cid     *cid.ID
	filters object.SearchFilters
}

// SelectRes groups resulting values of Select operation.
type SelectRes struct {
	addrList []*object.Address
}

// WithContainerID is a Select option to set the container id to search in.
func (p *SelectPrm) WithContainerID(cid *cid.ID) *SelectPrm {
	if p != nil {
		p.cid = cid
	}

	return p
}

// WithFilters is a Select option to set the object filters.
func (p *SelectPrm) WithFilters(fs object.SearchFilters) *SelectPrm {
	if p != nil {
		p.filters = fs
	}

	return p
}

// AddressList returns list of addresses of the selected objects.
func (r *SelectRes) AddressList() []*object.Address {
	return r.addrList
}

var ErrMissingContainerID = errors.New("missing container id field")

// Select selects the objects from DB with filtering.
func Select(db *DB, cid *cid.ID, fs object.SearchFilters) ([]*object.Address, error) {
	r, err := db.Select(new(SelectPrm).WithFilters(fs).WithContainerID(cid))
	if err != nil {
		return nil, err
	}

	return r.AddressList(), nil
}

// Select returns list of addresses of objects that match search filters.
func (db *DB) Select(prm *SelectPrm) (res *SelectRes, err error) {
	res = new(SelectRes)

	res.addrList, err = db.selectObjects(prm.cid, prm.filters)

	return res, err
}

func (db *DB) selectObjects(cid *cid.ID, fs object.SearchFilters) ([]*object.Address, error) {
	if cid == nil {
		return nil, ErrMissingContainerID
	}

	// TODO: consider the option of moving this check to a level higher than the metabase
	if blindlyProcess(fs) {
		return nil, nil
	}

	group, err := groupFilters(fs)
	if err != nil {
		return nil, err
	}

	// if there are conflicts in query and cid then it means that there is no
	// objects to match this query.
	if group.cid != nil && !cid.Equal(group.cid) {
		return nil, nil
	}

	// keep matched addresses in this cache
	// value equal to number (index+1) of latest matched filter
	mAddr := make(map[string]int)

	expLen := len(group.fastFilters) // expected value of matched filters in mAddr

	if len(group.fastFilters) == 0 {
		expLen = 1

		db.selectAll(cid, mAddr)
	} else {
		for i := range group.fastFilters {
			db.selectFastFilter(cid, group.fastFilters[i], mAddr, i)
		}
	}

	res := make([]*object.Address, 0, len(mAddr))

	for a, ind := range mAddr {
		if ind != expLen {
			continue // ignore objects with unmatched fast filters
		}

		addr, err := addressFromKey([]byte(a))
		if err != nil {
			// TODO: storage was broken, so we need to handle it
			return nil, err
		}

		if db.inGraveyard(addr) {
			continue // ignore removed objects
		}

		if !db.matchSlowFilters(addr, group.slowFilters) {
			continue // ignore objects with unmatched slow filters
		}

		res = append(res, addr)
	}

	return res, nil
}

// selectAll adds to resulting cache all available objects in metabase.
func (db *DB) selectAll(cid *cid.ID, to map[string]int) {
	prefix := cid.String() + "/"
	name := cidBucketKey(cid, primaryPrefix, nil)
	db.selectAllFromBucket(name, prefix, to, 0)

	name[0] = tombstonePrefix
	db.selectAllFromBucket(name, prefix, to, 0)

	name[0] = storageGroupPrefix
	db.selectAllFromBucket(name, prefix, to, 0)

	name[0] = parentPrefix
	db.selectAllFromBucket(name, prefix, to, 0)
}

// selectAllFromBucket goes through all keys in bucket and adds them in a
// resulting cache. Keys should be stringed object ids.
func (db *DB) selectAllFromBucket(name []byte, prefix string, to map[string]int, fNum int) {
	iter := db.newPrefixIterator(name)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		parts := splitKey(iter.Key())
		if len(parts) >= 2 {
			key := prefix + string(parts[1]) // consider using string builders from sync.Pool
			markAddressInCache(to, fNum, key)
		}
	}
}

func nextKey(key []byte) []byte {
	nxt := make([]byte, len(key))
	copy(nxt, key)

	for i := len(nxt) - 1; i >= 0; i-- {
		if nxt[i] != 0xFF {
			nxt[i]++
			return nxt
		}
	}

	// key consists of 0xFF bytes, return unchanged.
	return nxt
}

func (db *DB) newPrefixIterator(start []byte) *pebble.Iterator {
	var end []byte
	if len(start) != 0 {
		end = nextKey(start)
	}

	return db.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
}

// selectFastFilter makes fast optimized checks for well known buckets or
// looking through user attribute buckets otherwise.
func (db *DB) selectFastFilter(
	cid *cid.ID, // container we search on
	f object.SearchFilter, // fast filter
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	prefix := cid.String() + "/"

	switch f.Header() {
	case v2object.FilterHeaderObjectID:
		db.selectObjectID(f, cid, to, fNum)
	case v2object.FilterHeaderOwnerID:
		bucketName := cidBucketKey(cid, ownerPrefix, nil)
		db.selectFromFKBT(bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderPayloadHash:
		bucketName := cidBucketKey(cid, payloadHashPrefix, nil)
		db.selectFromList(bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderObjectType:
		for _, bucketName := range bucketNamesForType(cid, f.Operation(), f.Value()) {
			db.selectAllFromBucket(bucketName, prefix, to, fNum)
		}
	case v2object.FilterHeaderParent:
		bucketName := cidBucketKey(cid, parentPrefix, nil)
		db.selectFromList(bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderSplitID:
		bucketName := cidBucketKey(cid, splitPrefix, nil)
		db.selectFromList(bucketName, f, prefix, to, fNum)
	case v2object.FilterPropertyRoot:
		bucketName := cidBucketKey(cid, rootPrefix, nil)
		db.selectAllFromBucket(bucketName, prefix, to, fNum)
	case v2object.FilterPropertyPhy:
		db.selectAllFromBucket(cidBucketKey(cid, primaryPrefix, nil), prefix, to, fNum)
		db.selectAllFromBucket(cidBucketKey(cid, tombstonePrefix, nil), prefix, to, fNum)
		db.selectAllFromBucket(cidBucketKey(cid, storageGroupPrefix, nil), prefix, to, fNum)
	default: // user attribute
		bucketName := cidBucketKey(cid, attributePrefix, []byte(f.Header()))

		if f.Operation() == object.MatchNotPresent {
			db.selectOutsideFKBT(allBucketNames(cid), bucketName, f, prefix, to, fNum)
		} else {
			db.selectFromFKBT(bucketName, f, prefix, to, fNum)
		}
	}
}

// TODO: move to DB struct
var mBucketNaming = map[string][]func(*cid.ID) []byte{
	v2object.TypeRegular.String():      {primaryBucketName, parentBucketName},
	v2object.TypeTombstone.String():    {tombstoneBucketName},
	v2object.TypeStorageGroup.String(): {storageGroupBucketName},
}

func allBucketNames(cid *cid.ID) (names [][]byte) {
	for _, fns := range mBucketNaming {
		for _, fn := range fns {
			names = append(names, fn(cid))
		}
	}

	return
}

func bucketNamesForType(cid *cid.ID, mType object.SearchMatchType, typeVal string) (names [][]byte) {
	appendNames := func(key string) {
		fns, ok := mBucketNaming[key]
		if ok {
			for _, fn := range fns {
				names = append(names, fn(cid))
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
	}

	return
}

// selectFromList looks into <fkbt> index to find list of addresses to add in
// resulting cache.
func (db *DB) selectFromFKBT(
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

	iter := db.newPrefixIterator(name)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		parts := splitKey(iter.Key())
		if len(parts) < 4 || !matchFunc(f.Header(), parts[2], f.Value()) {
			continue
		}
		addr := prefix + string(parts[3])
		markAddressInCache(to, fNum, addr)
	}
}

// selectOutsideFKBT looks into all incl buckets to find list of addresses outside <fkbt> to add in
// resulting cache.
func (db *DB) selectOutsideFKBT(
	incl [][]byte, // buckets
	name []byte, // fkbt root bucket name
	f object.SearchFilter, // filter for operation and value
	prefix string, // prefix to create addr from oid in index
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	mExcl := make(map[string]struct{})

	iter := db.newPrefixIterator(name)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		parts := splitKey(iter.Key())
		if len(parts) >= 4 {
			addr := prefix + string(parts[3])
			mExcl[addr] = struct{}{}
		}
	}

	for i := range incl {
		iter := db.newPrefixIterator(incl[i])
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			parts := splitKey(iter.Key())
			if len(parts) >= 2 {
				addr := prefix + string(parts[1])
				if _, ok := mExcl[addr]; !ok {
					markAddressInCache(to, fNum, addr)
				}
			}
		}
	}
}

// selectFromList looks into <list> index to find list of addresses to add in
// resulting cache.
func (db *DB) selectFromList(
	name []byte, // list root bucket name
	f object.SearchFilter, // filter for operation and value
	prefix string, // prefix to create addr from oid in index
	to map[string]int, // resulting cache
	fNum int, // index of filter
) { //

	var (
		lst [][]byte
	)

	switch op := f.Operation(); op {
	case object.MatchStringEqual:
		bk := bucketKeyHelper(f.Header(), f.Value())
		iter := db.newPrefixIterator(appendKey(name, bk))
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			parts := splitKey(iter.Key())
			lst = append(lst, cloneBytes(parts[2]))
		}
	default:
		fMatch, ok := db.matchers[op]
		if !ok {
			db.log.Debug("unknown operation", zap.Uint32("operation", uint32(op)))

			return
		}

		iter := db.newPrefixIterator(name)
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			parts := splitKey(iter.Key())

			if len(parts) < 3 || !fMatch(f.Header(), parts[1], f.Value()) {
				continue
			}

			lst = append(lst, cloneBytes(parts[2]))
		}
	}

	for i := range lst {
		addr := prefix + string(lst[i])
		markAddressInCache(to, fNum, addr)
	}
}

// selectObjectID processes objectID filter with in-place optimizations.
func (db *DB) selectObjectID(
	f object.SearchFilter,
	cid *cid.ID,
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	prefix := cid.String() + "/"

	appendOID := func(oid string) {
		addrStr := prefix + string(oid)

		addr, err := addressFromKey([]byte(addrStr))
		if err != nil {
			db.log.Debug("can't decode object id address",
				zap.String("addr", addrStr),
				zap.String("error", err.Error()))

			return
		}

		ok, err := db.exists(addr)
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

		for _, bucketName := range bucketNamesForType(cid, object.MatchStringNotEqual, "") {
			// copy-paste from DB.selectAllFrom
			iter := db.newPrefixIterator(bucketName)
			defer iter.Close()

			for iter.First(); iter.Valid(); iter.Next() {
				parts := splitKey(iter.Key())
				if len(parts) >= 2 && fMatch(f.Header(), parts[1], f.Value()) {
					appendOID(string(parts[1]))
				}
			}
		}
	}
}

// matchSlowFilters return true if object header is matched by all slow filters.
func (db *DB) matchSlowFilters(addr *object.Address, f object.SearchFilters) bool {
	if len(f) == 0 {
		return true
	}

	obj, err := db.get(addr, true, false)
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
			data = obj.PayloadHomomorphicHash().Sum()
		case v2object.FilterHeaderCreationEpoch:
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, obj.CreationEpoch())
		case v2object.FilterHeaderPayloadLength:
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, obj.PayloadSize())
		default:
			continue // ignore unknown search attributes
		}

		if !matchFunc(f[i].Header(), data, f[i].Value()) {
			return false
		}
	}

	return true
}

// groupFilters divides filters in two groups: fast and slow. Fast filters
// processed by indexes and slow filters processed after by unmarshaling
// object headers.
func groupFilters(filters object.SearchFilters) (*filterGroup, error) {
	res := &filterGroup{
		fastFilters: make(object.SearchFilters, 0, len(filters)),
		slowFilters: make(object.SearchFilters, 0, len(filters)),
	}

	for i := range filters {
		switch filters[i].Header() {
		case v2object.FilterHeaderContainerID: // support deprecated field
			res.cid = cid.New()

			err := res.cid.Parse(filters[i].Value())
			if err != nil {
				return nil, fmt.Errorf("can't parse container id: %w", err)
			}
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

		// TODO: check other cases
		//  e.g. (a == b) && (a != b)
	}

	return false
}

// returns true if string key is a reserved system filter key.
func isSystemKey(key string) bool {
	// FIXME: version-dependent approach
	return strings.HasPrefix(key, v2object.ReservedFilterPrefix)
}
