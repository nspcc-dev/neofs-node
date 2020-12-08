package meta

import (
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/pkg/errors"
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
	filters object.SearchFilters
}

// SelectRes groups resulting values of Select operation.
type SelectRes struct {
	addrList []*object.Address
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

var ErrContainerNotInQuery = errors.New("search query does not contain container id filter")

// Select selects the objects from DB with filtering.
func Select(db *DB, fs object.SearchFilters) ([]*object.Address, error) {
	r, err := db.Select(new(SelectPrm).WithFilters(fs))
	if err != nil {
		return nil, err
	}

	return r.AddressList(), nil
}

// Select returns list of addresses of objects that match search filters.
func (db *DB) Select(prm *SelectPrm) (res *SelectRes, err error) {
	res = new(SelectRes)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.addrList, err = db.selectObjects(tx, prm.filters)

		return err
	})

	return res, err
}

func (db *DB) selectObjects(tx *bbolt.Tx, fs object.SearchFilters) ([]*object.Address, error) {
	group, err := groupFilters(fs)
	if err != nil {
		return nil, err
	}

	if group.cid == nil {
		return nil, ErrContainerNotInQuery
	}

	// keep matched addresses in this cache
	// value equal to number (index+1) of latest matched filter
	mAddr := make(map[string]int)

	expLen := len(group.fastFilters) // expected value of matched filters in mAddr

	if len(group.fastFilters) == 0 {
		expLen = 1

		db.selectAll(tx, group.cid, mAddr)
	} else {
		for i := range group.fastFilters {
			db.selectFastFilter(tx, group.cid, group.fastFilters[i], mAddr, i)
		}
	}

	res := make([]*object.Address, 0, len(mAddr))

	for a, ind := range mAddr {
		if ind != expLen {
			continue // ignore objects with unmatched fast filters
		}

		addr := object.NewAddress()
		if err := addr.Parse(a); err != nil {
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
		db.selectObjectID(tx, f, prefix, to, fNum)
	case v2object.FilterHeaderOwnerID:
		bucketName := ownerBucketName(cid)
		db.selectFromFKBT(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderPayloadHash:
		bucketName := payloadHashBucketName(cid)
		db.selectFromList(tx, bucketName, f, prefix, to, fNum)
	case v2object.FilterHeaderObjectType:
		var bucketName []byte

		switch f.Value() { // do it better after https://github.com/nspcc-dev/neofs-api/issues/84
		case "Regular":
			bucketName = primaryBucketName(cid)

			selectAllFromBucket(tx, bucketName, prefix, to, fNum)

			bucketName = parentBucketName(cid)
		case "Tombstone":
			bucketName = tombstoneBucketName(cid)
		case "StorageGroup":
			bucketName = storageGroupBucketName(cid)
		default:
			db.log.Debug("unknown object type", zap.String("type", f.Value()))

			return
		}

		selectAllFromBucket(tx, bucketName, prefix, to, fNum)
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
		db.selectFromFKBT(tx, bucketName, f, prefix, to, fNum)
	}
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

	switch f.Operation() {
	case object.MatchStringEqual:
	default:
		db.log.Debug("unknown operation", zap.Uint32("operation", uint32(f.Operation())))

		return
	}

	// warning: it works only for MatchStringEQ, for NotEQ you should iterate over
	// bkt and apply matchFunc, don't forget to implement this when it will be
	// needed. Right now it is not efficient to iterate over bucket
	// when there is only MatchStringEQ.
	lst, err := decodeList(bkt.Get(bucketKeyHelper(f.Header(), f.Value())))
	if err != nil {
		db.log.Debug("can't decode list bucket leaf", zap.String("error", err.Error()))
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
	prefix string,
	to map[string]int, // resulting cache
	fNum int, // index of filter
) {
	switch f.Operation() {
	case object.MatchStringEqual:
	default:
		db.log.Debug("unknown operation", zap.Uint32("operation", uint32(f.Operation())))

		return
	}

	// warning: it is in-place optimization and works only for MatchStringEQ,
	// for NotEQ you should iterate over bkt and apply matchFunc

	addrStr := prefix + f.Value()
	addr := object.NewAddress()

	err := addr.Parse(addrStr)
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

// matchSlowFilters return true if object header is matched by all slow filters.
func (db *DB) matchSlowFilters(tx *bbolt.Tx, addr *object.Address, f object.SearchFilters) bool {
	if len(f) == 0 {
		return true
	}

	obj, err := db.get(tx, addr, true)
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
		case v2object.FilterHeaderContainerID:
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
