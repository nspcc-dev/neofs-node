package meta

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"slices"

	"github.com/nspcc-dev/bbolt"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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
		addrList []oid.Address
		attrs    []string
		cursor   string
	)

	if len(filters) > 0 {
		attrs = append(attrs, filters[0].Header())
	}

	for {
		ofs, c, err := objectcore.PreprocessSearchQuery(filters, attrs, cursor)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", objectcore.ErrInvalidSearchQuery, err)
		}

		res, newCursor, err := db.Search(cnr, ofs, attrs, c, math.MaxUint16)
		if err != nil {
			return nil, fmt.Errorf("call metabase: %w", err)
		}
		for i := range res {
			addrList = append(addrList, oid.NewAddress(cnr, res[i].ID))
		}
		if len(newCursor) == 0 {
			break
		}
		cursor = base64.StdEncoding.EncodeToString(newCursor)
	}

	return addrList, nil
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

// CollectRawWithAttribute allows to fetch the list of objects precisely
// matching given attribute and value. No expiration/lock/tombstone checks
// are made.
func (db *DB) CollectRawWithAttribute(cnr cid.ID, attr string, val []byte) ([]oid.ID, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	var (
		metaBktKey = metaBucketKey(cnr)
		res        []oid.ID
	)

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		var metaBkt = tx.Bucket(metaBktKey)
		if metaBkt == nil {
			return nil
		}
		for v := range iterAttrVal(metaBkt.Cursor(), attr, val) {
			res = append(res, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, err
}

func iterPrefixedIDs(cur *bbolt.Cursor, pref []byte, offset oid.ID) func(yield func(oid.ID) bool) {
	var k []byte

	if offset.IsZero() {
		k, _ = cur.Seek(pref)
	} else {
		var seekPos = slices.Concat(pref, offset[:])

		k, _ = cur.Seek(seekPos)
		if bytes.Equal(k, seekPos) {
			k, _ = cur.Next() // We are looking for objects _after_ the offset.
		}
	}

	return func(yield func(oid.ID) bool) {
		for ; bytes.HasPrefix(k, pref); k, _ = cur.Next() {
			id, err := oid.DecodeBytes(k[len(pref):])
			if err != nil {
				continue
			}
			if !yield(id) {
				break
			}
		}
	}
}

func iterAttrVal(cur *bbolt.Cursor, attr string, val []byte) func(yield func(oid.ID) bool) {
	var pref = slices.Concat([]byte{metaPrefixAttrIDPlain}, []byte(attr),
		objectcore.MetaAttributeDelimiter, val, objectcore.MetaAttributeDelimiter)

	return iterPrefixedIDs(cur, pref, oid.ID{})
}
