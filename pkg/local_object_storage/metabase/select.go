package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type (
	// filterGroup is a structure that have search filters grouped by access
	// method. We have fast filters that looks for indexes and do not unmarshal
	// objects, and slow filters, that applied after fast filters created
	// smaller set of objects to check.
	filterGroup struct {
		cid         *container.ID
		fastFilters object.SearchFilters
		slowFilters object.SearchFilters
	}
)

// SelectPrm groups the parameters of Select operation.
type SelectPrm struct {
	cid     *container.ID
	filters object.SearchFilters
}

// SelectRes groups resulting values of Select operation.
type SelectRes struct {
	addrList []*object.Address
}

// WithContainerID is a Select option to set the container id to search in.
func (p *SelectPrm) WithContainerID(cid *container.ID) *SelectPrm {
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
func Select(db *DB, cid *container.ID, fs object.SearchFilters) ([]*object.Address, error) {
	r, err := db.Select(new(SelectPrm).WithFilters(fs).WithContainerID(cid))
	if err != nil {
		return nil, err
	}

	return r.AddressList(), nil
}

// Select returns list of addresses of objects that match search filters.
func (db *DB) Select(prm *SelectPrm) (res *SelectRes, err error) {
	res = new(SelectRes)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.addrList, err = db.selectObjects(tx, prm.cid, prm.filters)

		return err
	})

	return res, err
}

func (db *DB) selectObjects(tx *bbolt.Tx, cid *container.ID, fs object.SearchFilters) ([]*object.Address, error) {
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

		db.selectAll(tx, cid, mAddr)
	} else {
		for i := range group.fastFilters {
			db.selectFastFilter(tx, cid, group.fastFilters[i], mAddr, i)
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

		if inGraveyard(tx, addr) {
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
func (db *DB) selectAll(tx *bbolt.Tx, cid *container.ID, to map[string]int) {
	prefix := cid.String() + "/"

	selectAllFromBucket(tx, primaryBucketName(cid), prefix, to, 0)
	selectAllFromBucket(tx, tombstoneBucketName(cid), prefix, to, 0)
	selectAllFromBucket(tx, storageGroupBucketName(cid), prefix, to, 0)
	selectAllFromBucket(tx, parentBucketName(cid), prefix, to, 0)
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
	cid *container.ID, // container we search on
	f object.SearchFilter, // fast filter
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	prefix := cid.String() + "/"

	switch f.Header() {
	case v2object.FilterHeaderObjectID:
		db.selectObjectID(tx, f, cid, to, fNum)
	case v2object.FilterHeaderOwnerID:
		bucketName := ownerBucketName(cid)
		db.selectFromFKBT(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderPayloadHash:
		bucketName := payloadHashBucketName(cid)
		db.selectFromList(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderObjectType:
		for _, bucketName := range bucketNamesForType(cid, f.Operation(), f.Value()) {
			selectAllFromBucket(tx, bucketName, prefix, to, fNum)
		}
	case v2object.FilterHeaderParent:
		bucketName := parentBucketName(cid)
		db.selectFromList(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderSplitID:
		bucketName := splitBucketName(cid)
		db.selectFromList(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterPropertyRoot:
		selectAllFromBucket(tx, rootBucketName(cid), prefix, to, fNum)
	case v2object.FilterPropertyPhy:
		selectAllFromBucket(tx, primaryBucketName(cid), prefix, to, fNum)
		selectAllFromBucket(tx, tombstoneBucketName(cid), prefix, to, fNum)
		selectAllFromBucket(tx, storageGroupBucketName(cid), prefix, to, fNum)
	default: // user attribute
		bucketName := attributeBucketName(cid, f.Header())

		if f.Operation() == object.MatchNotPresent {
			selectOutsideFKBT(tx, allBucketNames(cid), bucketName, f, prefix, to, fNum)
		} else {
			db.selectFromFKBT(tx, bucketName, f, prefix, to, fNum)
		}
	}
}

// TODO: move to DB struct
var mBucketNaming = map[string][]func(*container.ID) []byte{
	v2object.TypeRegular.String():      {primaryBucketName, parentBucketName},
	v2object.TypeTombstone.String():    {tombstoneBucketName},
	v2object.TypeStorageGroup.String(): {storageGroupBucketName},
}

func allBucketNames(cid *container.ID) (names [][]byte) {
	for _, fns := range mBucketNaming {
		for _, fn := range fns {
			names = append(names, fn(cid))
		}
	}

	return
}

func bucketNamesForType(cid *container.ID, mType object.SearchMatchType, typeVal string) (names [][]byte) {
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

	err := fkbtRoot.ForEach(func(k, _ []byte) error {
		if matchFunc(f.Header(), k, f.Value()) {
			fkbtLeaf := fkbtRoot.Bucket(k)
			if fkbtLeaf == nil {
				return nil
			}

			return fkbtLeaf.ForEach(func(k, _ []byte) error {
				addr := prefix + string(k)
				markAddressInCache(to, fNum, addr)

				return nil
			})
		}

		return nil
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

		if err = bkt.ForEach(func(key, val []byte) error {
			if !fMatch(f.Header(), key, f.Value()) {
				return nil
			}

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
	cid *container.ID,
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

		for _, bucketName := range bucketNamesForType(cid, object.MatchStringNotEqual, "") {
			// copy-paste from DB.selectAllFrom
			bkt := tx.Bucket(bucketName)
			if bkt == nil {
				return
			}

			err := bkt.ForEach(func(k, v []byte) error {
				if oid := string(k); fMatch(f.Header(), k, f.Value()) {
					appendOID(oid)
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
func (db *DB) matchSlowFilters(tx *bbolt.Tx, addr *object.Address, f object.SearchFilters) bool {
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
			res.cid = container.NewID()

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
