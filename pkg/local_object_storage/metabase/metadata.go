package meta

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

const (
	metaPrefixID = byte(iota)
	metaPrefixAttrIDInt
	metaPrefixAttrIDPlain
	metaPrefixIDAttr
)

const (
	intValLen      = 33                                   // prefix byte for sign + fixed256 in metaPrefixAttrIDInt
	attrIDFixedLen = 1 + oid.Size + attributeDelimiterLen // prefix first
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

// PutMetadataForObject fills object meta-data indexes using bbolt transaction.
// Transaction must be writable. Additional bucket for container's meta-data
// may be created using {255, CID...} form as a key.
func PutMetadataForObject(tx *bbolt.Tx, hdr object.Object, phy bool) error {
	metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(hdr.GetContainerID()))
	if err != nil {
		return fmt.Errorf("create meta bucket for container: %w", err)
	}
	id := hdr.GetID()
	idk := [1 + oid.Size]byte{metaPrefixID}
	copy(idk[1:], id[:])
	if err := metaBkt.Put(idk[:], nil); err != nil {
		return fmt.Errorf("put object ID to container's meta bucket: %w", err)
	}

	var keyBuf keyBuffer
	var ver version.Version
	if v := hdr.Version(); v != nil {
		ver = *v
	}
	if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterVersion, ver.String()); err != nil {
		return err
	}
	owner := hdr.Owner()
	if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterOwnerID, string(owner[:])); err != nil {
		return err
	}
	typ := hdr.Type()
	if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterType, typ.String()); err != nil {
		return err
	}
	creationEpoch := hdr.CreationEpoch()
	if err = putIntAttribute(metaBkt, &keyBuf, id, object.FilterCreationEpoch, strconv.FormatUint(creationEpoch, 10), new(big.Int).SetUint64(creationEpoch)); err != nil {
		return err
	}
	payloadLen := hdr.PayloadSize()
	if err = putIntAttribute(metaBkt, &keyBuf, id, object.FilterPayloadSize, strconv.FormatUint(payloadLen, 10), new(big.Int).SetUint64(payloadLen)); err != nil {
		return err
	}
	if h, ok := hdr.PayloadChecksum(); ok {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterPayloadChecksum, string(h.Value())); err != nil {
			return err
		}
	}
	if h, ok := hdr.PayloadHomomorphicHash(); ok {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterPayloadHomomorphicHash, string(h.Value())); err != nil {
			return err
		}
	}
	if splitID := hdr.SplitID().ToV2(); len(splitID) > 0 {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterSplitID, string(splitID)); err != nil {
			return err
		}
	}
	if firstID := hdr.GetFirstID(); !firstID.IsZero() {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterFirstSplitObject, string(firstID[:])); err != nil {
			return err
		}
	}
	if parentID := hdr.GetParentID(); !parentID.IsZero() {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterParentID, string(parentID[:])); err != nil {
			return err
		}
	}
	if !hdr.HasParent() && hdr.Type() == object.TypeRegular {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterRoot, binPropMarker); err != nil {
			return err
		}
	}
	if phy {
		if err = putPlainAttribute(metaBkt, &keyBuf, id, object.FilterPhysical, binPropMarker); err != nil {
			return err
		}
	}
	attrs := hdr.Attributes()
	for i := range attrs {
		ak, av := attrs[i].Key(), attrs[i].Value()
		if n, isInt := parseInt(av); isInt && intWithinLimits(n) {
			err = putIntAttribute(metaBkt, &keyBuf, id, ak, av, n)
		} else {
			err = putPlainAttribute(metaBkt, &keyBuf, id, ak, av)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteMetadata(tx *bbolt.Tx, l *zap.Logger, cnr cid.ID, id oid.ID, isParent bool) (uint64, error) {
	metaBkt := tx.Bucket(metaBucketKey(cnr))
	if metaBkt == nil {
		return 0, nil
	}
	var err error
	var parent oid.ID
	var size uint64
	if !isParent {
		v := metaBkt.Get(slices.Concat([]byte{metaPrefixIDAttr}, id[:], []byte(object.FilterPhysical), objectcore.MetaAttributeDelimiter, []byte(binPropMarker)))
		if v == nil {
			return 0, nil
		}
	}
	pref := slices.Concat([]byte{metaPrefixID}, id[:])
	if err := metaBkt.Delete(pref); err != nil {
		return 0, err
	}
	// removed keys must be pre-collected according to BoltDB docs.
	var ks [][]byte
	pref[0] = metaPrefixIDAttr
	c := metaBkt.Cursor()
	for kIDAttr, _ := c.Seek(pref); bytes.HasPrefix(kIDAttr, pref); kIDAttr, _ = c.Next() {
		attrK, attrV, found := bytes.Cut(kIDAttr[len(pref):], objectcore.MetaAttributeDelimiter)
		if !found {
			return 0, fmt.Errorf("invalid key with prefix 0x%X in meta bucket: missing delimiter", kIDAttr[0])
		}
		switch kStr := string(attrK); kStr {
		case object.FilterType:
			var typ object.Type
			if !isParent && typ.DecodeString(string(attrV)) {
				err := delUniqueIndexes(tx, cnr, id, typ, isParent)
				if err != nil {
					return 0, err
				}
			}
		case object.FilterParentID:
			if len(attrV) == oid.Size {
				parent = oid.ID(attrV)
			}
		case object.FilterPayloadSize:
			size, _ = strconv.ParseUint(string(attrV), 10, 64)
		default:
		}
		kAttrID := make([]byte, len(kIDAttr)+attributeDelimiterLen)
		kAttrID[0] = metaPrefixAttrIDPlain
		off := 1 + copy(kAttrID[1:], kIDAttr[1+oid.Size:])
		off += copy(kAttrID[off:], objectcore.MetaAttributeDelimiter)
		copy(kAttrID[off:], id[:])
		ks = append(ks, kIDAttr, kAttrID)
		if n, ok := new(big.Int).SetString(string(attrV), 10); ok && intWithinLimits(n) {
			kAttrIDInt := make([]byte, 1+len(attrK)+attributeDelimiterLen+intValLen+oid.Size)
			kAttrIDInt[0] = metaPrefixAttrIDInt
			off := 1 + copy(kAttrIDInt[1:], attrK)
			off += copy(kAttrIDInt[off:], objectcore.MetaAttributeDelimiter)
			putInt(kAttrIDInt[off:off+intValLen], n)
			copy(kAttrIDInt[off+intValLen:], id[:])
			ks = append(ks, kAttrIDInt)
		}
	}
	for i := range ks {
		if err := metaBkt.Delete(ks[i]); err != nil {
			return 0, err
		}
	}

	if !parent.IsZero() {
		_, err = deleteMetadata(tx, l, cnr, parent, true)
		if err != nil {
			l.Warn("parent removal",
				zap.Stringer("child", oid.NewAddress(cnr, id)),
				zap.Stringer("parent", oid.NewAddress(cnr, parent)),
				zap.Error(err))
		}
	}

	return size, nil
}

// Search selects up to count container's objects from the given container
// matching the specified filters.
func (db *DB) Search(cnr cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	var res []client.SearchResultItem
	var newCursor []byte
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

func (db *DB) search(cnr cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	var res []client.SearchResultItem
	var newCursor []byte
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

func (db *DB) searchTx(tx *bbolt.Tx, cnr cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	metaBkt := tx.Bucket(metaBucketKey(cnr))
	if metaBkt == nil {
		return nil, nil, nil
	}

	primCursor := metaBkt.Cursor()
	primKey, _ := primCursor.Seek(cursor.PrimarySeekKey)
	if bytes.Equal(primKey, cursor.PrimarySeekKey) { // points to the last response element, so go next
		primKey, _ = primCursor.Next()
	}
	if primKey == nil {
		return nil, nil, nil
	}

	var keyBuf keyBuffer
	attrSkr := &metaAttributeSeeker{keyBuf: &keyBuf, bkt: metaBkt}
	curEpoch := db.epochState.CurrentEpoch()
	var gcCheck objectcore.AdditionalObjectChecker = func(id oid.ID) (match bool) {
		return objectStatus(tx, oid.NewAddress(cnr, id), curEpoch) == 0
	}
	resHolder := objectcore.SearchResult{Objects: make([]client.SearchResultItem, 0, count)}
	handleKV := objectcore.MetaDataKVHandler(&resHolder, attrSkr, gcCheck, fs, attrs, cursor, count)

	for ; bytes.HasPrefix(primKey, cursor.PrimaryKeysPrefix); primKey, _ = primCursor.Next() {
		if !handleKV(primKey, nil) {
			break
		}
	}

	return resHolder.Objects, resHolder.UpdatedSearchCursor, resHolder.Err
}

// TODO: can be merged with filtered code?
func (db *DB) searchUnfiltered(cnr cid.ID, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	res := make([]client.SearchResultItem, count)
	var n uint16
	var newCursor []byte
	curEpoch := db.epochState.CurrentEpoch()
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		mb := tx.Bucket(metaBucketKey(cnr))
		if mb == nil {
			return nil
		}

		mbc := mb.Cursor()
		k, _ := mbc.Seek(cursor.PrimarySeekKey)
		if cursor != nil && bytes.Equal(k, cursor.PrimarySeekKey) { // cursor is the last response element, so go next
			k, _ = mbc.Next()
		}
		for ; len(k) > 0 && k[0] == metaPrefixID; k, _ = mbc.Next() {
			if n == count { // there are still elements
				newCursor = res[n-1].ID[:]
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

func metaBucketKey(cnr cid.ID) []byte {
	k := [1 + cid.Size]byte{metadataPrefix}
	copy(k[1:], cnr[:])
	return k[:]
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

func intWithinLimits(n *big.Int) bool { return n.Cmp(maxUint256Neg) >= 0 && n.Cmp(maxUint256) <= 0 }

// makes PREFIX_ATTR_DELIM_VAL_OID with unset VAL space, and returns offset of
// the VAL. Reuses previously allocated buffer if it is sufficient.
func prepareMetaAttrIDKey(buf *keyBuffer, id oid.ID, attr string, valLen int, intAttr bool) ([]byte, int) {
	kln := attrIDFixedLen + len(attr) + valLen
	if !intAttr {
		kln += attributeDelimiterLen
	}
	k := buf.alloc(kln)
	if intAttr {
		k[0] = metaPrefixAttrIDInt
	} else {
		k[0] = metaPrefixAttrIDPlain
	}
	off := 1 + copy(k[1:], attr)
	off += copy(k[off:], objectcore.MetaAttributeDelimiter)
	valOff := off
	off += valLen
	if !intAttr {
		off += copy(k[off:], objectcore.MetaAttributeDelimiter)
	}
	copy(k[off:], id[:])
	return k, valOff
}

// similar to prepareMetaAttrIDKey but makes PREFIX_OID_ATTR_DELIM_VAL.
func prepareMetaIDAttrKey(buf *keyBuffer, id oid.ID, attr string, valLen int) []byte {
	k := buf.alloc(attrIDFixedLen + len(attr) + valLen)
	k[0] = metaPrefixIDAttr
	off := 1 + copy(k[1:], id[:])
	off += copy(k[off:], attr)
	copy(k[off:], objectcore.MetaAttributeDelimiter)
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

func putIntAttribute(bkt *bbolt.Bucket, buf *keyBuffer, id oid.ID, attr, origin string, parsed *big.Int) error {
	k, off := prepareMetaAttrIDKey(buf, id, attr, intValLen, true)
	putInt(k[off:off+intValLen], parsed)
	if err := bkt.Put(k, nil); err != nil {
		return fmt.Errorf("put integer object attribute %q to container's meta bucket (attribute-to-ID): %w", attr, err)
	}
	return putPlainAttribute(bkt, buf, id, attr, origin)
}

type metaAttributeSeeker struct {
	keyBuf *keyBuffer
	bkt    *bbolt.Bucket
	crsr   *bbolt.Cursor
}

func (x *metaAttributeSeeker) Get(id []byte, attr string) (attributeValue []byte, err error) {
	pref := x.keyBuf.alloc(attrIDFixedLen + len(attr))
	pref[0] = metaPrefixIDAttr
	off := 1 + copy(pref[1:], id)
	off += copy(pref[off:], attr)
	copy(pref[off:], objectcore.MetaAttributeDelimiter)
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
