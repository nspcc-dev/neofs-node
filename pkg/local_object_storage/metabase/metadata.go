package meta

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"go.etcd.io/bbolt"
)

const (
	metaPrefixID = byte(iota)
	metaPrefixAttrIDInt
	metaPrefixAttrIDPlain
	metaPrefixIDAttr
)

const (
	intValLen      = 33                              // prefix byte for sign + fixed256 in metaPrefixAttrIDInt
	attrIDFixedLen = 1 + oid.Size + utf8DelimiterLen // prefix first
)

const binPropMarker = "1" // ROOT, PHY, etc.

var (
	maxUint256 = new(big.Int).SetBytes([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
	maxUint256Neg = new(big.Int).Neg(maxUint256)
)

func invalidMetaBucketKeyErr(key []byte, cause error) error {
	return fmt.Errorf("invalid meta bucket key (prefix 0x%X): %w", key[0], cause)
}

// TODO: fill on migration.
func putMetadata(tx *bbolt.Tx, cnr cid.ID, id oid.ID, ver version.Version, owner user.ID, typ object.Type, creationEpoch uint64,
	payloadLen uint64, pldHash, pldHmmHash, splitID []byte, parentID, firstID oid.ID, attrs []object.Attribute,
	root, phy bool) error {
	metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cnr))
	if err != nil {
		return fmt.Errorf("create meta bucket for container: %w", err)
	}
	idk := [1 + oid.Size]byte{metaPrefixID}
	copy(idk[1:], id[:])
	if err := metaBkt.Put(idk[:], nil); err != nil {
		return fmt.Errorf("put object ID to container's meta bucket: %w", err)
	}

	var keyBuf keyBuffer
	if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterVersion, ver.String()); err != nil {
		return err
	}
	if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterOwnerID, string(owner[:])); err != nil {
		return err
	}
	if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterType, typ.String()); err != nil {
		return err
	}
	if err = putIntAttribute(metaBkt, &keyBuf, id, object.FilterCreationEpoch, new(big.Int).SetUint64(creationEpoch)); err != nil {
		return err
	}
	if err = putIntAttribute(metaBkt, &keyBuf, id, object.FilterPayloadSize, new(big.Int).SetUint64(payloadLen)); err != nil {
		return err
	}
	if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterPayloadChecksum, string(pldHash)); err != nil {
		return err
	}
	if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterPayloadHomomorphicHash, string(pldHmmHash)); err != nil {
		return err
	}
	if len(splitID) > 0 {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterSplitID, string(splitID)); err != nil {
			return err
		}
	}
	if !firstID.IsZero() {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterFirstSplitObject, string(firstID[:])); err != nil {
			return err
		}
	}
	if !parentID.IsZero() {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterParentID, string(parentID[:])); err != nil {
			return err
		}
	}
	if root {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterRoot, binPropMarker); err != nil {
			return err
		}
	}
	if phy {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterPhysical, binPropMarker); err != nil {
			return err
		}
	}
	for i := range attrs {
		ak, av := attrs[i].Key(), attrs[i].Value()
		if n, isInt := parseInt(av); isInt && n.Cmp(maxUint256Neg) >= 0 && n.Cmp(maxUint256) <= 0 {
			err = putIntAttribute(metaBkt, &keyBuf, id, ak, n)
		} else {
			err = putPlainAttribute(metaBkt, &keyBuf, id, ak, av)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteMetadata(tx *bbolt.Tx, cnr cid.ID, id oid.ID) error {
	metaBkt := tx.Bucket(metaBucketKey(cnr))
	if metaBkt == nil {
		return nil
	}
	pref := slices.Concat([]byte{metaPrefixID}, id[:])
	if err := metaBkt.Delete(pref); err != nil {
		return err
	}
	// removed keys must be pre-collected according to BoltDB docs.
	var ks [][]byte
	pref[0] = metaPrefixIDAttr
	c := metaBkt.Cursor()
	for kIDAttr, _ := c.Seek(pref); bytes.HasPrefix(kIDAttr, pref); kIDAttr, _ = c.Next() {
		sepInd := bytes.LastIndex(kIDAttr, utf8Delimiter)
		if sepInd < 0 {
			return fmt.Errorf("invalid key with prefix 0x%X in meta bucket: missing delimiter", kIDAttr[0])
		}
		kAttrID := slices.Clone(kIDAttr)
		kAttrID[0] = metaPrefixAttrIDPlain
		copy(kAttrID[1:], kIDAttr[1+oid.Size:])
		copy(kAttrID[len(kAttrID)-oid.Size:], id[:])
		if val := kIDAttr[sepInd+utf8DelimiterLen:]; len(val) == intValLen && val[0] <= 1 { // most likely integer
			kAttrIDInt := slices.Clone(kAttrID)
			kAttrIDInt[0] = metaPrefixAttrIDInt
			ks = append(ks, kIDAttr, kAttrID, kAttrIDInt)
		} else { // non-int
			ks = append(ks, kIDAttr, kAttrID)
		}
	}
	for i := range ks {
		if err := metaBkt.Delete(ks[i]); err != nil {
			return err
		}
	}
	return nil
}

// SearchCursor is a cursor used for continuous search in the DB.
type SearchCursor struct {
	Key      []byte // PREFIX_ATTR_DELIM_VAL_ID, prefix is unset
	ValIDOff int
}

// NewSearchCursorFromString decodes cursor from the string according to the
// primary attribute.
func NewSearchCursorFromString(s, primAttr string) (*SearchCursor, error) {
	if s == "" {
		return nil, nil
	}
	b := make([]byte, 1+base64.StdEncoding.DecodedLen(len(s)))
	n, err := base64.StdEncoding.Decode(b[1:], []byte(s))
	if err != nil {
		return nil, fmt.Errorf("decode cursor from Base64: %w", err)
	}
	b = b[:1+n]
	if primAttr == "" {
		if n != oid.Size {
			return nil, fmt.Errorf("wrong OID cursor len %d", n)
		}
		return &SearchCursor{Key: b}, nil
	}
	if n > object.MaxHeaderLen {
		return nil, fmt.Errorf("cursor len %d exceeds the limit %d", n, object.MaxHeaderLen)
	}
	ind := bytes.Index(b[1:], utf8Delimiter) // 1st is prefix
	if ind < 0 {
		return nil, errors.New("missing delimiter")
	}
	if !bytes.Equal(b[1:1+ind], []byte(primAttr)) {
		return nil, errors.New("wrong attribute")
	}
	var res SearchCursor
	res.ValIDOff = 1 + len(primAttr) + len(utf8Delimiter)
	if len(b[res.ValIDOff:]) <= oid.Size {
		return nil, errors.New("missing value")
	}
	res.Key = b
	return &res, nil
}

// Search selects up to count container's objects from the given container
// matching the specified filters.
func (db *DB) Search(cnr cid.ID, fs object.SearchFilters, attrs []string, cursor *SearchCursor, count uint16) ([]client.SearchResultItem, *SearchCursor, error) {
	if blindlyProcess(fs) {
		return nil, nil, nil
	}
	var res []client.SearchResultItem
	var newCursor *SearchCursor
	var err error
	if len(fs) == 0 {
		res, newCursor, err = db.searchUnfiltered(cnr, cursor, count)
	} else {
		res, newCursor, err = db.search(cnr, fs, attrs, cursor, count)
	}
	if err != nil {
		return nil, nil, err
	}
	return res, newCursor, nil
}

func (db *DB) search(cnr cid.ID, fs object.SearchFilters, attrs []string, cursor *SearchCursor, count uint16) ([]client.SearchResultItem, *SearchCursor, error) {
	var res []client.SearchResultItem
	var newCursor *SearchCursor
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		var err error
		res, newCursor, err = db.searchTx(tx, cnr, fs, attrs, cursor, count)
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("view BoltDB: %w", err)
	}
	return res, newCursor, nil
}

func (db *DB) searchTx(tx *bbolt.Tx, cnr cid.ID, fs object.SearchFilters, attrs []string, cursor *SearchCursor, count uint16) ([]client.SearchResultItem, *SearchCursor, error) {
	metaBkt := tx.Bucket(metaBucketKey(cnr))
	if metaBkt == nil {
		return nil, nil, nil
	}
	// TODO: make as much as possible outside the Bolt tx
	primMatcher, primVal := convertFilterValue(fs[0])
	intPrimMatcher := objectcore.IsIntegerSearchOp(primMatcher)
	notPresentPrimMatcher := primMatcher == object.MatchNotPresent
	primAttr := fs[0].Header() // attribute emptiness already prevented
	var primSeekKey, primSeekPrefix []byte
	var prevResOID, prevResPrimVal []byte
	if notPresentPrimMatcher {
		if cursor != nil {
			primSeekKey = cursor.Key
		} else {
			primSeekKey = make([]byte, 1)
		}
		primSeekKey[0] = metaPrefixID
		primSeekPrefix = primSeekKey[:1]
	} else if cursor != nil {
		primSeekKey = cursor.Key
		if intPrimMatcher {
			primSeekKey[0] = metaPrefixAttrIDInt
		} else {
			primSeekKey[0] = metaPrefixAttrIDPlain
		}
		primSeekPrefix = primSeekKey[:cursor.ValIDOff]
		valID := cursor.Key[cursor.ValIDOff:]
		prevResPrimVal, prevResOID = valID[:len(valID)-oid.Size], valID[len(valID)-oid.Size:]
	} else {
		if primMatcher == object.MatchStringEqual || primMatcher == object.MatchCommonPrefix ||
			primMatcher == object.MatchNumGT || primMatcher == object.MatchNumGE {
			var err error
			if primSeekKey, primSeekPrefix, err = seekKeyForAttribute(primAttr, primVal); err != nil {
				return nil, nil, fmt.Errorf("invalid primary filter value: %w", err)
			}
		} else {
			prefixByte := metaPrefixAttrIDPlain
			if intPrimMatcher {
				prefixByte = metaPrefixAttrIDInt
			}
			primSeekKey = slices.Concat([]byte{prefixByte}, []byte(primAttr), utf8Delimiter)
			primSeekPrefix = primSeekKey
		}
	}

	primCursor := metaBkt.Cursor()
	primKey, _ := primCursor.Seek(primSeekKey)
	if bytes.Equal(primKey, primSeekKey) { // points to the last response element, so go next
		primKey, _ = primCursor.Next()
	}
	if primKey == nil {
		return nil, nil, nil
	}

	res := make([]client.SearchResultItem, count)
	collectedPrimVals := make([][]byte, count)
	collectedPrimKeys := make([][]byte, count) // TODO: can be done w/o slice
	var n uint16
	var more bool
	var id, dbVal []byte
	var keyBuf keyBuffer
	attrSkr := &metaAttributeSeeker{keyBuf: &keyBuf, bkt: metaBkt}
	curEpoch := db.epochState.CurrentEpoch()
nextPrimKey:
	for ; bytes.HasPrefix(primKey, primSeekPrefix); primKey, _ = primCursor.Next() {
		if notPresentPrimMatcher {
			if id = primKey[1:]; len(id) != oid.Size {
				return nil, nil, invalidMetaBucketKeyErr(primKey, fmt.Errorf("invalid OID len %d", len(id)))
			}
		} else { // apply primary filter
			valID := primKey[len(primSeekPrefix):] // VAL_OID
			if len(valID) <= oid.Size {
				return nil, nil, invalidMetaBucketKeyErr(primKey, fmt.Errorf("too small VAL_OID len %d", len(valID)))
			}
			dbVal, id = valID[:len(valID)-oid.Size], valID[len(valID)-oid.Size:]
			if !intPrimMatcher && primKey[0] == metaPrefixAttrIDInt {
				var err error
				if dbVal, err = restoreIntAttributeVal(dbVal); err != nil {
					return nil, nil, invalidMetaBucketKeyErr(primKey, fmt.Errorf("invalid integer value: %w", err))
				}
			}
			for i := range fs {
				// there may be several filters by primary key, e.g. N >= 10 && N <= 20. We
				// check them immediately before moving through the DB.
				attr := fs[i].Header()
				if i > 0 && attr != primAttr {
					continue
				}
				mch, val := convertFilterValue(fs[i])
				checkedDBVal, fltVal, err := combineValues(attr, dbVal, val) // TODO: deduplicate DB value preparation
				if err != nil {
					return nil, nil, fmt.Errorf("invalid key in meta bucket: invalid attribute %s value: %w", attr, err)
				}
				if !matchValues(checkedDBVal, mch, fltVal) {
					continue nextPrimKey
				}
				// TODO: attribute value can be requested, it can be collected here, or we can
				//  detect earlier when an object goes beyond the already collected result. The
				//  code can become even more complex. Same below
			}
		}
		// apply other filters
		for i := range fs {
			if !notPresentPrimMatcher && i == 0 { // 1st already checked
				continue
			}
			attr := fs[i].Header() // emptiness already prevented
			for j := 1; j < i; j++ {
				if fs[j].Header() == attr { // already match, checked in loop below
					continue
				}
			}
			var err error
			if dbVal, err = attrSkr.get(id, attr); err != nil {
				return nil, nil, err
			}
			var dbValInt *[]byte // nil means not yet checked, pointer to nil means non-int
			for j := i; j < len(fs); j++ {
				if j > 0 && fs[j].Header() != attr {
					continue
				}
				m, val := convertFilterValue(fs[j])
				if dbVal == nil {
					if m == object.MatchNotPresent {
						continue
					}
					continue nextPrimKey
				}
				if m == object.MatchNotPresent {
					continue nextPrimKey
				}
				if dbValInt == nil {
					if len(dbVal) != intValLen {
						dbValInt = new([]byte)
					} else {
						// do the same as for primary attribute, but unlike there, here we don't know
						// whether the attribute is expected to be integer or not.
						dbValInt = new([]byte)
						if attrSkr.isInt(id, attr, dbVal) {
							var err error
							if *dbValInt, err = restoreIntAttributeVal(dbVal); err != nil {
								return nil, nil, invalidMetaBucketKeyErr(primKey, fmt.Errorf("invalid integer value: %w", err))
							}
						}
					}
				}
				var checkedDBVal []byte
				if objectcore.IsIntegerSearchOp(m) {
					if *dbValInt == nil {
						continue nextPrimKey
					}
					checkedDBVal = dbVal
				} else if *dbValInt != nil {
					checkedDBVal = *dbValInt
				} else {
					checkedDBVal = dbVal
				}
				checkedDBVal, fltVal, err := combineValues(attr, checkedDBVal, val) // TODO: deduplicate DB value preparation
				if err != nil {
					return nil, nil, invalidMetaBucketKeyErr(primKey, fmt.Errorf("invalid attribute %s value: %w", attr, err))
				}
				if !matchValues(checkedDBVal, m, fltVal) {
					continue nextPrimKey
				}
			}
		}
		if objectStatus(tx, oid.NewAddress(cnr, oid.ID(id)), curEpoch) > 0 { // GC-ed
			continue nextPrimKey
		}
		// object matches, collect attributes
		collected := make([]string, len(attrs))
		var primDBVal []byte
		var insertI uint16
		if len(attrs) > 0 {
			var err error
			if primDBVal, err = attrSkr.get(id, attrs[0]); err != nil {
				return nil, nil, err
			}
			if cursor != nil { // can be < than previous response chunk
				if c := bytes.Compare(primDBVal, prevResPrimVal); c < 0 || c == 0 && bytes.Compare(id, prevResOID) <= 0 {
					continue nextPrimKey
				}
				// note that if both values are integers, they are already sorted. Otherwise, the order is undefined.
				// We could treat non-int values as < then the int ones, but the code would have grown huge
			}
			for i := range n {
				if c := bytes.Compare(primDBVal, collectedPrimVals[i]); c < 0 || c == 0 && bytes.Compare(id, res[i].ID[:]) < 0 {
					break
				}
				if insertI++; insertI == count {
					more = true
					continue nextPrimKey
				}
			}
			if collected[0], err = attrSkr.restoreVal(id, attrs[0], primDBVal); err != nil {
				return nil, nil, err
			}
		} else {
			if cursor != nil { // can be < than previous response chunk
				if bytes.Compare(id, prevResOID) <= 0 {
					continue nextPrimKey
				}
			}
			for i := insertI; i < n; i++ {
				if bytes.Compare(id, res[i].ID[:]) >= 0 {
					if insertI++; insertI == count {
						more = true
						continue nextPrimKey
					}
				}
			}
		}
		for i := 1; i < len(attrs); i++ {
			val, err := attrSkr.get(id, attrs[i])
			if err != nil {
				return nil, nil, err
			}
			if collected[i], err = attrSkr.restoreVal(id, attrs[i], val); err != nil {
				return nil, nil, err
			}
		}
		if n == count {
			more = true
			if len(attrs) > 0 {
				break
			} // else "later" objects may have "less" ID, so we should continue
		}
		copy(res[insertI+1:], res[insertI:])
		res[insertI].ID = oid.ID(id)
		res[insertI].Attributes = collected
		copy(collectedPrimVals[insertI+1:], collectedPrimVals[insertI:])
		collectedPrimVals[insertI] = primDBVal
		copy(collectedPrimKeys[insertI+1:], collectedPrimKeys[insertI:])
		collectedPrimKeys[insertI] = primKey
		if n < count {
			n++
		}
	}
	var newCursor *SearchCursor
	if more {
		newCursor = &SearchCursor{
			Key:      slices.Clone(collectedPrimKeys[n-1][1:]),
			ValIDOff: len(primSeekPrefix),
		}
	}
	return res[:n], newCursor, nil
}

// TODO: can be merged with filtered code?
func (db *DB) searchUnfiltered(cnr cid.ID, cursor *SearchCursor, count uint16) ([]client.SearchResultItem, *SearchCursor, error) {
	var seekKey []byte
	if cursor != nil {
		seekKey = cursor.Key
		seekKey[0] = metaPrefixID
	} else {
		seekKey = []byte{metaPrefixID}
	}
	res := make([]client.SearchResultItem, count)
	var n uint16
	var newCursor *SearchCursor
	curEpoch := db.epochState.CurrentEpoch()
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		mb := tx.Bucket(metaBucketKey(cnr))
		if mb == nil {
			return nil
		}

		mbc := mb.Cursor()
		k, _ := mbc.Seek(seekKey)
		if cursor != nil && bytes.Equal(k, seekKey) { // cursor is the last response element, so go next
			k, _ = mbc.Next()
		}
		for ; k[0] == metaPrefixID; k, _ = mbc.Next() {
			if n == count { // there are still elements
				newCursor = &SearchCursor{Key: res[n-1].ID[:]}
				return nil
			}
			if len(k) != oid.Size+1 {
				return invalidMetaBucketKeyErr(k, fmt.Errorf("unexpected object key len %d", len(k)))
			}
			res[n].ID = oid.ID(k[1:])
			if objectStatus(tx, oid.NewAddress(cnr, res[n].ID), curEpoch) > 0 { // GC-ed
				continue
			}
			n++
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("view BoltDB: %w", err)
	}
	return res[:n], newCursor, nil
}

func seekKeyForAttribute(attr, fltVal string) ([]byte, []byte, error) {
	var dbVal []byte
	switch attr {
	default:
		if n, ok := new(big.Int).SetString(fltVal, 10); ok && n.Cmp(maxUint256Neg) >= 0 && n.Cmp(maxUint256) <= 0 {
			key := make([]byte, 1+len(attr)+utf8DelimiterLen+intValLen) // prefix 1st
			key[0] = metaPrefixAttrIDInt
			off := 1 + copy(key[1:], attr)
			off += copy(key[off:], utf8Delimiter)
			prefix := key[:off]
			putInt(key[off:off+intValLen], n)
			return key, prefix, nil
		}
		dbVal = []byte(fltVal)
	case object.FilterOwnerID, object.FilterFirstSplitObject, object.FilterParentID:
		var err error
		if dbVal, err = base58.Decode(fltVal); err != nil {
			return nil, nil, fmt.Errorf("decode %q attribute value from Base58: %w", attr, err)
		}
	case object.FilterPayloadChecksum, object.FilterPayloadHomomorphicHash:
		var err error
		if dbVal, err = hex.DecodeString(fltVal); err != nil {
			return nil, nil, fmt.Errorf("decode %q attribute value from HEX: %w", attr, err)
		}
	case object.FilterSplitID:
		uid, err := uuid.Parse(fltVal)
		if err != nil {
			return nil, nil, fmt.Errorf("decode %q UUID attribute: %w", attr, err)
		}
		dbVal = uid[:]
	case object.FilterVersion, object.FilterType, object.FilterRoot, object.FilterPhysical:
	}
	key := make([]byte, 1+len(attr)+utf8DelimiterLen+len(dbVal)) // prefix 1st
	key[0] = metaPrefixAttrIDPlain
	off := 1 + copy(key[1:], attr)
	off += copy(key[off:], utf8Delimiter)
	prefix := key[:off]
	copy(key[off:], dbVal)
	return key, prefix, nil
}

// combines attribute's DB and NeoFS API SearchV2 values to the matchable
// format. Returns DB errors only.
func combineValues(attr string, dbVal []byte, fltVal string) ([]byte, []byte, error) {
	switch attr {
	case object.FilterOwnerID:
		if len(dbVal) != user.IDSize {
			return nil, nil, fmt.Errorf("invalid owner len %d != %d", len(dbVal), user.IDSize)
		}
		if b, _ := base58.Decode(fltVal); len(b) == user.IDSize {
			return dbVal, b, nil
		}
		// consider filter 'owner PREFIX N':
		//  - any object matches it
		//  - decoded filter byte is always 21 while the DB one is always 53
		// so we'd get false mismatch. To avoid this, we have to decode each DB val.
		dbVal = []byte(base58.Encode(dbVal))
	case object.FilterFirstSplitObject, object.FilterParentID:
		if len(dbVal) != oid.Size {
			return nil, nil, fmt.Errorf("invalid OID len %d != %d", len(dbVal), oid.Size)
		}
		if b, _ := base58.Decode(fltVal); len(b) == oid.Size {
			return dbVal, b, nil
		}
		// same as owner
		dbVal = []byte(base58.Encode(dbVal))
	case object.FilterPayloadChecksum:
		if len(dbVal) != sha256.Size {
			return nil, nil, fmt.Errorf("invalid payload checksum len %d != %d", len(dbVal), sha256.Size)
		}
		if b, err := hex.DecodeString(fltVal); err == nil {
			return dbVal, b, nil
		}
		dbVal = []byte(hex.EncodeToString(dbVal))
	case object.FilterPayloadHomomorphicHash:
		if len(dbVal) != tz.Size {
			return nil, nil, fmt.Errorf("invalid payload homomorphic hash len %d != %d", len(dbVal), tz.Size)
		}
		if b, err := hex.DecodeString(fltVal); err == nil {
			return dbVal, b, nil
		}
		dbVal = []byte(hex.EncodeToString(dbVal))
	case object.FilterSplitID:
		if len(dbVal) != 16 {
			return nil, nil, fmt.Errorf("invalid split ID len %d != 16", len(dbVal))
		}
		uid, err := uuid.Parse(fltVal)
		if err == nil {
			return dbVal, uid[:], nil
		}
		copy(uid[:], dbVal)
		dbVal = []byte(uid.String())
	}
	return dbVal, []byte(fltVal), nil
}

func metaBucketKey(cnr cid.ID) []byte {
	k := [1 + cid.Size]byte{metadataPrefix}
	copy(k[1:], cnr[:])
	return k[:]
}

func intBytes(n *big.Int) []byte {
	b := make([]byte, intValLen)
	putInt(b, n)
	return b
}

func putInt(b []byte, n *big.Int) {
	if len(b) < intValLen {
		panic(fmt.Errorf("insufficient buffer len %d", len(b)))
	}
	neg := n.Sign() < 0
	if neg {
		b[0] = 0
	} else {
		b[0] = 1
	}
	n.FillBytes(b[1:intValLen])
	if neg {
		for i := range b[1:] {
			b[1+i] = ^b[1+i]
		}
	}
}

func restoreIntAttributeVal(b []byte) ([]byte, error) {
	n, err := restoreIntAttribute(b)
	if err != nil {
		return nil, err
	}
	return []byte(n.String()), nil
}

func restoreIntAttribute(b []byte) (*big.Int, error) {
	if len(b) != intValLen {
		return nil, fmt.Errorf("invalid len %d", len(b))
	}
	switch b[0] {
	default:
		return nil, fmt.Errorf("invalid sign byte %d", b[0])
	case 1:
		return new(big.Int).SetBytes(b[1:]), nil
	case 0:
		cp := slices.Clone(b[1:])
		for i := range cp {
			cp[i] = ^cp[i]
		}
		n := new(big.Int).SetBytes(cp)
		return n.Neg(n), nil
	}
}

// matches object attribute's search query value to the DB-stored one. Matcher
// must be supported but not [object.MatchNotPresent].
func matchValues(dbVal []byte, matcher object.SearchMatchType, fltVal []byte) bool {
	switch {
	default:
		return false // TODO: check whether supported in blindlyProcess. Then panic here
	case matcher == object.MatchNotPresent:
		panic(errors.New("unexpected matcher NOT_PRESENT"))
	case matcher == object.MatchStringEqual:
		return bytes.Equal(dbVal, fltVal)
	case matcher == object.MatchStringNotEqual:
		return !bytes.Equal(dbVal, fltVal)
	case matcher == object.MatchCommonPrefix:
		return bytes.HasPrefix(dbVal, fltVal)
	case objectcore.IsIntegerSearchOp(matcher):
		var n big.Int
		return n.UnmarshalText(fltVal) == nil && intMatches(dbVal, matcher, &n)
	}
}

func intMatches(dbVal []byte, matcher object.SearchMatchType, fltVal *big.Int) bool {
	if c := fltVal.Cmp(maxUint256); c >= 0 {
		if matcher == object.MatchNumGT || c > 0 && matcher == object.MatchNumGE {
			return false
		}
		if matcher == object.MatchNumLE || c > 0 && matcher == object.MatchNumLT {
			return true
		}
	}
	if c := fltVal.Cmp(maxUint256Neg); c <= 0 {
		if matcher == object.MatchNumLT || c < 0 && matcher == object.MatchNumLE {
			return false
		}
		if matcher == object.MatchNumGE || c < 0 && matcher == object.MatchNumGT {
			return true
		}
	}
	fltValBytes := intBytes(fltVal) // TODO: buffer can be useful for other filters
	switch matcher {
	default:
		panic(fmt.Errorf("unexpected integer matcher %d", matcher))
	case object.MatchNumGT:
		return bytes.Compare(dbVal, fltValBytes) > 0
	case object.MatchNumGE:
		return bytes.Compare(dbVal, fltValBytes) >= 0
	case object.MatchNumLT:
		return bytes.Compare(dbVal, fltValBytes) < 0
	case object.MatchNumLE:
		return bytes.Compare(dbVal, fltValBytes) <= 0
	}
}

// makes PREFIX_ATTR_DELIM_VAL_OID with unset VAL space, and returns offset of
// the VAL. Reuses previously allocated buffer if it is sufficient.
func prepareMetaAttrIDKey(buf *keyBuffer, id oid.ID, attr string, valLen int, intAttr bool) ([]byte, int) {
	k := buf.alloc(attrIDFixedLen + len(attr) + valLen)
	if intAttr {
		k[0] = metaPrefixAttrIDInt
	} else {
		k[0] = metaPrefixAttrIDPlain
	}
	off := 1 + copy(k[1:], attr)
	off += copy(k[off:], utf8Delimiter)
	valOff := off
	off += valLen
	copy(k[off:], id[:])
	return k, valOff
}

// similar to prepareMetaAttrIDKey but makes PREFIX_OID_ATTR_DELIM_VAL.
func prepareMetaIDAttrKey(buf *keyBuffer, id oid.ID, attr string, valLen int) []byte {
	k := buf.alloc(attrIDFixedLen + len(attr) + valLen)
	k[0] = metaPrefixIDAttr
	off := 1 + copy(k[1:], id[:])
	off += copy(k[off:], attr)
	copy(k[off:], utf8Delimiter)
	return k
}

func putPlainAttribute[V []byte | string](bkt *bbolt.Bucket, buf *keyBuffer, id oid.ID, attr string, val V) error {
	k, off := prepareMetaAttrIDKey(buf, id, attr, len(val), false)
	copy(k[off:], val)
	if err := bkt.Put(k, nil); err != nil {
		return fmt.Errorf("put object attribute %q to container's meta bucket (attribute-to-ID): %w", attr, err)
	}
	k = prepareMetaIDAttrKey(buf, id, attr, len(val)) // TODO: ATTR_DELIM_VAL can just be moved
	copy(k[len(k)-len(val):], val)
	if err := bkt.Put(k, nil); err != nil {
		return fmt.Errorf("put object attribute %q to container's meta bucket (ID-to-attribute): %w", attr, err) // TODO: distinguishable context
	}
	return nil
}

func putIntAttribute(bkt *bbolt.Bucket, buf *keyBuffer, id oid.ID, attr string, val *big.Int) error {
	k, off := prepareMetaAttrIDKey(buf, id, attr, intValLen, true)
	putInt(k[off:off+intValLen], val)
	if err := bkt.Put(k, nil); err != nil {
		return fmt.Errorf("put integer object attribute %q to container's meta bucket (attribute-to-ID): %w", attr, err)
	}
	k = prepareMetaIDAttrKey(buf, id, attr, intValLen) // TODO: ATTR_DELIM_VAL can just be moved
	putInt(k[len(k)-intValLen:], val)
	if err := bkt.Put(k, nil); err != nil {
		return fmt.Errorf("put integer object attribute %q to container's meta bucket (ID-to-attribute): %w", attr, err)
	}
	return nil
}

type metaAttributeSeeker struct {
	keyBuf *keyBuffer
	bkt    *bbolt.Bucket
	crsr   *bbolt.Cursor
}

func (x *metaAttributeSeeker) get(id []byte, attr string) ([]byte, error) {
	pref := x.keyBuf.alloc(attrIDFixedLen + len(attr))
	pref[0] = metaPrefixIDAttr
	off := 1 + copy(pref[1:], id)
	off += copy(pref[off:], attr)
	copy(pref[off:], utf8Delimiter)
	if x.crsr == nil {
		x.crsr = x.bkt.Cursor()
	}
	key, _ := x.crsr.Seek(pref)
	if !bytes.HasPrefix(key, pref) {
		return nil, nil
	}
	if len(key[len(pref):]) == 0 {
		return nil, invalidMetaBucketKeyErr(key, errors.New("missing attribute value"))
	}
	return key[len(pref):], nil
}

func (x *metaAttributeSeeker) isInt(id []byte, attr string, val []byte) bool {
	key := x.keyBuf.alloc(attrIDFixedLen + len(attr) + len(val))
	key[0] = metaPrefixAttrIDInt
	off := 1 + copy(key[1:], attr)
	off += copy(key[off:], utf8Delimiter)
	off += copy(key[off:], val)
	copy(key[off:], id)
	return x.bkt.Get(key) != nil
}

func (x *metaAttributeSeeker) restoreVal(id []byte, attr string, stored []byte) (string, error) {
	if len(stored) == intValLen && x.isInt(id, attr, stored) {
		n, err := restoreIntAttribute(stored)
		if err != nil {
			return "", invalidMetaBucketKeyErr([]byte{metaPrefixAttrIDInt}, fmt.Errorf("invalid integer value: %w", err))
		}
		return n.String(), nil
	}
	switch attr {
	case object.FilterOwnerID, object.FilterFirstSplitObject, object.FilterParentID:
		return base58.Encode(stored), nil
	case object.FilterPayloadChecksum, object.FilterPayloadHomomorphicHash:
		return hex.EncodeToString(stored), nil
	case object.FilterSplitID:
		uid, err := uuid.ParseBytes(stored)
		if err != nil {
			return "", invalidMetaBucketKeyErr([]byte{metaPrefixAttrIDPlain}, fmt.Errorf("decode split ID: decode UUID: %w", err))
		}
		return uid.String(), nil
	}
	return string(stored), nil
}

func convertFilterValue(f object.SearchFilter) (object.SearchMatchType, string) {
	if attr := f.Header(); attr == object.FilterRoot || attr == object.FilterPhysical {
		return object.MatchStringEqual, binPropMarker
	}
	return f.Operation(), f.Value()
}

// CalculateCursor calculates cursor for the given last search result item.
func CalculateCursor(fs object.SearchFilters, lastItem client.SearchResultItem) (SearchCursor, error) {
	if len(lastItem.Attributes) == 0 || len(fs) == 0 || fs[0].Operation() == object.MatchNotPresent {
		return SearchCursor{Key: lastItem.ID[:]}, nil
	}
	attr := fs[0].Header()
	var val []byte
	switch attr {
	default:
		if n, ok := new(big.Int).SetString(lastItem.Attributes[0], 10); ok {
			var res SearchCursor
			res.Key = make([]byte, len(attr)+utf8DelimiterLen+intValLen+oid.Size)
			off := copy(res.Key, attr)
			res.ValIDOff = off + copy(res.Key[off:], utf8Delimiter)
			putInt(res.Key[res.ValIDOff:res.ValIDOff+intValLen], n)
			copy(res.Key[res.ValIDOff+intValLen:], lastItem.ID[:])
			return res, nil
		}
	case object.FilterOwnerID, object.FilterFirstSplitObject, object.FilterParentID:
		var err error
		if val, err = base58.Decode(lastItem.Attributes[0]); err != nil {
			return SearchCursor{}, fmt.Errorf("decode %q attribute value from Base58: %w", attr, err)
		}
	case object.FilterPayloadChecksum, object.FilterPayloadHomomorphicHash:
		ln := hex.DecodedLen(len(lastItem.Attributes[0]))
		if attr == object.FilterPayloadChecksum && ln != sha256.Size || attr == object.FilterPayloadHomomorphicHash && ln != tz.Size {
			return SearchCursor{}, fmt.Errorf("wrong %q attribute decoded len %d", attr, ln)
		}
		var res SearchCursor
		res.Key = make([]byte, len(attr)+utf8DelimiterLen+ln+oid.Size)
		off := copy(res.Key, attr)
		res.ValIDOff = off + copy(res.Key[off:], utf8Delimiter)
		var err error
		if _, err = hex.Decode(res.Key[res.ValIDOff:], []byte(lastItem.Attributes[0])); err != nil {
			return SearchCursor{}, fmt.Errorf("decode %q attribute from HEX: %w", attr, err)
		}
		copy(res.Key[res.ValIDOff+ln:], lastItem.ID[:])
		return res, nil
	case object.FilterSplitID:
		uid, err := uuid.Parse(lastItem.Attributes[0])
		if err != nil {
			return SearchCursor{}, fmt.Errorf("decode %q attribute from HEX: %w", attr, err)
		}
		val = uid[:]
	case object.FilterVersion, object.FilterType:
	}
	if val == nil {
		val = []byte(lastItem.Attributes[0])
	}
	var res SearchCursor
	res.Key = make([]byte, len(attr)+utf8DelimiterLen+len(val)+oid.Size)
	off := copy(res.Key, attr)
	res.ValIDOff = off + copy(res.Key[off:], utf8Delimiter)
	off = res.ValIDOff + copy(res.Key[res.ValIDOff:], val)
	copy(res.Key[off:], lastItem.ID[:])
	return res, nil
}
