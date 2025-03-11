package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"os"
	"path"
	"slices"
	"strconv"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"go.uber.org/zap"
)

type containerStorage struct {
	m           sync.RWMutex
	mptOpsBatch map[string][]byte

	path string
	mpt  *mpt.Trie
	db   storage.Store
}

func (s *containerStorage) drop() error {
	s.m.Lock()
	defer s.m.Unlock()

	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("close container storage: %w", err)
	}

	err = os.RemoveAll(s.path)
	if err != nil {
		return fmt.Errorf("remove container storage: %w", err)
	}

	return nil
}

type eventWithMptKVs struct {
	ev            objEvent
	additionalKVs map[string][]byte
}

func (s *containerStorage) putObjects(ctx context.Context, l *zap.Logger, bInd uint32, ee []objEvent, net NeoFSNetwork) {
	s.m.Lock()
	defer s.m.Unlock()

	// raw indexes are responsible for object validation and only after
	// object is taken as a valid one, it goes to the slower-on-read MPT
	// storage via objCh
	objCh := make(chan eventWithMptKVs, len(ee))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.putRawIndexes(ctx, l, ee, net, objCh)
		if err != nil {
			l.Error("failed to put raw indexes", zap.Error(err))
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.putMPTIndexes(bInd, objCh)
		if err != nil {
			l.Error("failed to put mpt indexes", zap.Error(err))
		}
	}()
	wg.Wait()
}

// lock should be taken.
func (s *containerStorage) putMPTIndexes(bInd uint32, ch <-chan eventWithMptKVs) error {
	for evWithKeys := range ch {
		maps.Copy(s.mptOpsBatch, evWithKeys.additionalKVs)

		e := evWithKeys.ev
		commsuffix := e.oID[:]

		// batching that is implemented for MPT ignores key's first byte

		s.mptOpsBatch[string(append([]byte{0, oidIndex}, commsuffix...))] = []byte{}
		if len(e.deletedObjects) > 0 {
			s.mptOpsBatch[string(append([]byte{0, deletedIndex}, commsuffix...))] = e.deletedObjects
		}
		if len(e.lockedObjects) > 0 {
			s.mptOpsBatch[string(append([]byte{0, lockedIndex}, commsuffix...))] = e.lockedObjects
		}
		s.mptOpsBatch[string(append([]byte{0, sizeIndex}, commsuffix...))] = e.size.Bytes()
		if len(e.firstObject) > 0 {
			s.mptOpsBatch[string(append([]byte{0, firstPartIndex}, commsuffix...))] = e.firstObject
		}
		if len(e.prevObject) > 0 {
			s.mptOpsBatch[string(append([]byte{0, previousPartIndex}, commsuffix...))] = e.prevObject
		}
		if e.typ != objectsdk.TypeRegular {
			s.mptOpsBatch[string(append([]byte{0, typeIndex}, commsuffix...))] = []byte{byte(e.typ)}
		}
	}

	root := s.mpt.StateRoot()
	s.mpt.Store.Put([]byte{rootKey}, root[:])

	_, err := s.mpt.PutBatch(mpt.MapToMPTBatch(s.mptOpsBatch))
	if err != nil {
		return fmt.Errorf("put batch to MPT storage: %w", err)
	}
	clear(s.mptOpsBatch)

	s.mpt.Flush(bInd)

	return nil
}

// lock should be taken.
func (s *containerStorage) putRawIndexes(ctx context.Context, l *zap.Logger, ee []objEvent, net NeoFSNetwork, res chan<- eventWithMptKVs) (finalErr error) {
	batch := make(map[string][]byte)
	defer func() {
		close(res)

		if finalErr == nil && len(batch) > 0 {
			err := s.db.PutChangeSet(batch, nil)
			if err != nil {
				finalErr = fmt.Errorf("put change set to DB: %w", err)
			}
		}
	}()

	for _, e := range ee {
		err := isOpAllowed(s.db, e)
		if err != nil {
			l.Warn("skip object", zap.Stringer("oid", e.oID), zap.String("reason", err.Error()))
			continue
		}

		evWithMpt := eventWithMptKVs{ev: e}

		h, err := net.Head(ctx, e.cID, e.oID)
		if err != nil {
			// TODO define behavior with status (non-network) errors; maybe it is near #3140
			return fmt.Errorf("HEAD %s object: %w", e.oID, err)
		}

		commsuffix := e.oID[:]

		batch[string(append([]byte{oidIndex}, commsuffix...))] = []byte{}
		if len(e.deletedObjects) > 0 {
			batch[string(append([]byte{deletedIndex}, commsuffix...))] = e.deletedObjects
			evWithMpt.additionalKVs, err = deleteObjectsOps(batch, s.db, e.deletedObjects)
			if err != nil {
				l.Error("cleaning deleted object", zap.Stringer("oid", e.oID), zap.Error(err))
				continue
			}
		}
		if len(e.lockedObjects) > 0 {
			batch[string(append([]byte{lockedIndex}, commsuffix...))] = e.lockedObjects

			for locked := range slices.Chunk(e.lockedObjects, oid.Size) {
				batch[string(append([]byte{lockedByIndex}, locked...))] = commsuffix
			}
		}

		err = object.VerifyHeaderForMetadata(h)
		if err != nil {
			l.Error("header verification", zap.Stringer("oid", e.oID), zap.Error(err))
			continue
		}

		res <- evWithMpt

		fillObjectIndex(batch, h)
	}

	return finalErr
}

func isOpAllowed(db storage.Store, e objEvent) error {
	if len(e.deletedObjects) == 0 && len(e.lockedObjects) == 0 {
		return nil
	}

	key := make([]byte, 1+oid.Size)

	for obj := range slices.Chunk(e.deletedObjects, oid.Size) {
		copy(key[1:], obj)

		// delete object that does not exist
		key[0] = oidIndex
		_, err := db.Get(key)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				return fmt.Errorf("%s object-to-delete is missing", oid.ID(obj))
			}
			return fmt.Errorf("%s object-to-delete's presence check: %w", oid.ID(obj), err)
		}

		// delete object that is locked
		key[0] = lockedByIndex
		v, err := db.Get(key)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				continue
			}
			return fmt.Errorf("%s object-to-delete's lock status check: %w", oid.ID(obj), err)
		}
		return fmt.Errorf("%s object-to-delete is locked by %s", oid.ID(obj), oid.ID(v))
	}

	for obj := range slices.Chunk(e.lockedObjects, oid.Size) {
		copy(key[1:], obj)

		// lock object that does not exist
		key[0] = oidIndex
		_, err := db.Get(key)
		if err != nil {
			return fmt.Errorf("%s object-to-lock's presence check: %w", oid.ID(obj), err)
		}
	}

	return nil
}

const binPropertyMarker = "1" // ROOT, PHY, etc.

func fillObjectIndex(batch map[string][]byte, h objectsdk.Object) {
	id := h.GetID()
	typ := h.Type()
	owner := h.Owner()
	creationEpoch := h.CreationEpoch()
	pSize := h.PayloadSize()
	fPart := h.GetFirstID()
	parID := h.GetParentID()
	hasParent := h.Parent() != nil
	phy := hasParent || (fPart.IsZero() && parID.IsZero())
	pldHash, _ := h.PayloadChecksum()
	var ver version.Version
	if v := h.Version(); v != nil {
		ver = *v
	}
	var pldHmmHash []byte
	if hash, ok := h.PayloadHomomorphicHash(); ok {
		pldHmmHash = hash.Value()
	}

	oidKey := [1 + oid.Size]byte{oidIndex}
	copy(oidKey[1:], id[:])
	batch[string(oidKey[:])] = []byte{}

	putPlainAttribute(batch, id, objectsdk.FilterVersion, ver.String())
	putPlainAttribute(batch, id, objectsdk.FilterOwnerID, string(owner[:]))
	putPlainAttribute(batch, id, objectsdk.FilterType, typ.String())
	putIntAttribute(batch, id, objectsdk.FilterCreationEpoch, strconv.FormatUint(creationEpoch, 10), new(big.Int).SetUint64(creationEpoch))
	putIntAttribute(batch, id, objectsdk.FilterPayloadSize, strconv.FormatUint(pSize, 10), new(big.Int).SetUint64(pSize))
	putPlainAttribute(batch, id, objectsdk.FilterPayloadChecksum, string(pldHash.Value()))
	putPlainAttribute(batch, id, objectsdk.FilterPayloadHomomorphicHash, string(pldHmmHash))
	if !fPart.IsZero() {
		putPlainAttribute(batch, id, objectsdk.FilterFirstSplitObject, string(fPart[:]))
	}
	if !parID.IsZero() {
		putPlainAttribute(batch, id, objectsdk.FilterParentID, string(parID[:]))
	}
	if !hasParent && typ == objectsdk.TypeRegular {
		putPlainAttribute(batch, id, objectsdk.FilterRoot, binPropertyMarker)
	}
	if phy {
		putPlainAttribute(batch, id, objectsdk.FilterPhysical, binPropertyMarker)
	}
	for _, a := range h.Attributes() {
		ak, av := a.Key(), a.Value()
		if n, isInt := parseInt(av); isInt && intWithinLimits(n) {
			putIntAttribute(batch, id, ak, av, n)
		} else {
			putPlainAttribute(batch, id, ak, av)
		}
	}
}

func deleteObjectsOps(dbKV map[string][]byte, s storage.Store, objects []byte) (map[string][]byte, error) {
	rng := storage.SeekRange{}
	mptKV := make(map[string][]byte)

	// nil value means "delete" operation

	for len(objects) > 0 {
		o := objects[:oid.Size]
		objects = objects[oid.Size:]
		rng.Start = append([]byte{oidIndex}, o...)
		stopKey := lastObjectKey(o)

		var objFound bool
		var err error

		s.Seek(rng, func(k, v []byte) bool {
			if bytes.Compare(k, stopKey) > 0 {
				return false
			}
			if !bytes.HasPrefix(k[1:], o) {
				return true
			}

			if !objFound {
				objFound = true

				// size index is the only index that is common for both storages
				// but is stored in completely different forms
				mptKV[string(append([]byte{0, sizeIndex}, o...))] = nil
			}

			dbKV[string(k)] = nil

			switch k[0] {
			// DB-only keys
			case firstPartIndex, previousPartIndex, typeIndex:
			// common keys for DB and MPT storages
			case oidIndex, deletedIndex, lockedIndex:
				mptKV[string(append([]byte{0}, k...))] = nil
			// DB reversed indexes
			case oidToAttrIndex:
				withoutOID := k[1+oid.Size:]
				i := bytes.Index(withoutOID, object.AttributeDelimiter)
				if i < 0 {
					err = fmt.Errorf("unexpected attribute index without delimeter: %s", string(k))
					return false
				}
				attrK := withoutOID[:i]
				attrV := withoutOID[i+attributeDelimiterLen:]

				// drop reverse plain index
				keyToDrop := make([]byte, 0, len(k)+len(object.AttributeDelimiter))
				keyToDrop = append(keyToDrop, attrPlainToOIDIndex)
				keyToDrop = append(keyToDrop, withoutOID...)
				keyToDrop = append(keyToDrop, object.AttributeDelimiter...)
				keyToDrop = append(keyToDrop, o...)

				dbKV[string(keyToDrop)] = nil

				if vInt, isInt := parseInt(string(attrV)); isInt && intWithinLimits(vInt) {
					keyToDrop = slices.Grow(keyToDrop, 1+len(attrK)+attributeDelimiterLen+intValLen+oid.Size)
					// drop reverse int index
					keyToDrop = keyToDrop[:0]
					keyToDrop = append(keyToDrop, attrIntToOIDIndex)
					keyToDrop = append(keyToDrop, attrK...)
					keyToDrop = append(keyToDrop, object.AttributeDelimiter...)
					keyToDrop = keyToDrop[:len(keyToDrop)+intValLen]
					putBigInt(keyToDrop[len(keyToDrop)-intValLen:], vInt)
					keyToDrop = append(keyToDrop, o...)

					dbKV[string(keyToDrop)] = nil
				}
			default:
				err = fmt.Errorf("unexpected index prefix: %d", k[0])
				return false
			}

			return true
		})
		if err != nil {
			return nil, err
		}
	}

	return mptKV, nil
}

// lastObjectKey returns the least possible key in sorted DB list that
// proves there will not be information about the object anymore.
func lastObjectKey(rawOID []byte) []byte {
	res := make([]byte, 0, len(rawOID)+1)
	res = append(res, lastEnumIndex-1)

	return append(res, rawOID...)
}

func storageForContainer(rootPath string, cID cid.ID) (*containerStorage, error) {
	p := path.Join(rootPath, cID.EncodeToString())

	st, err := storage.NewBoltDBStore(dbconfig.BoltDBOptions{FilePath: p, ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("open bolt store at %q: %w", p, err)
	}

	var prevRootNode mpt.Node
	root, err := st.Get([]byte{rootKey})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		if err != nil {
			return nil, fmt.Errorf("get state root from db: %w", err)
		}

		if len(root) != util.Uint256Size {
			return nil, fmt.Errorf("root hash from db is %d bytes long, expect %d", len(root), util.Uint256Size)
		}

		prevRootNode = mpt.NewHashNode([util.Uint256Size]byte(root))
	}

	return &containerStorage{
		path:        p,
		mpt:         mpt.NewTrie(prevRootNode, mpt.ModeLatest, storage.NewMemCachedStore(st)),
		db:          st,
		mptOpsBatch: make(map[string][]byte),
	}, nil
}

const (
	intValLen             = 33 // prefix byte for sign + fixed256 in attrIntToOIDIndex
	attributeDelimiterLen = 1
)

func parseInt(s string) (*big.Int, bool) {
	return new(big.Int).SetString(s, 10)
}

var (
	maxUint256 = new(big.Int).SetBytes([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
	maxUint256Neg = new(big.Int).Neg(maxUint256)
)

func intWithinLimits(n *big.Int) bool {
	return n.Cmp(maxUint256Neg) >= 0 && n.Cmp(maxUint256) <= 0
}

func putPlainAttribute(batch map[string][]byte, id oid.ID, k, v string) {
	resKey := make([]byte, 0, 1+2*attributeDelimiterLen+oid.Size+len(k)+len(v))

	// PREFIX_OID_ATTR_DELIM_VAL
	resKey = append(resKey, oidToAttrIndex)
	resKey = append(resKey, id[:]...)
	resKey = append(resKey, k...)
	resKey = append(resKey, object.AttributeDelimiter...)
	resKey = append(resKey, v...)

	batch[string(resKey)] = []byte{}
	resKey = resKey[:0]

	// PREFIX_ATTR_DELIM_VAL_DELIM_OID
	resKey = append(resKey, attrPlainToOIDIndex)
	resKey = append(resKey, k...)
	resKey = append(resKey, object.AttributeDelimiter...)
	resKey = append(resKey, v...)
	resKey = append(resKey, object.AttributeDelimiter...)
	resKey = append(resKey, id[:]...)

	batch[string(resKey)] = []byte{}
}

func putIntAttribute(batch map[string][]byte, id oid.ID, k, vRaw string, vParsed *big.Int) {
	putPlainAttribute(batch, id, k, vRaw)

	resKey := make([]byte, 0, 1+len(k)+attributeDelimiterLen+intValLen+oid.Size)

	// PREFIX_ATTR_DELIM_VAL_OID
	resKey = append(resKey, attrIntToOIDIndex)
	resKey = append(resKey, k...)
	resKey = append(resKey, object.AttributeDelimiter...)

	resKey = resKey[:len(resKey)+intValLen]
	putBigInt(resKey[len(resKey)-intValLen:], vParsed)

	resKey = append(resKey, id[:]...)
	batch[string(resKey)] = []byte{}
}

func putBigInt(b []byte, bInt *big.Int) {
	neg := bInt.Sign() < 0
	if neg {
		b[0] = 0
	} else {
		b[0] = 1
	}
	bInt.FillBytes(b[1:])
	if neg {
		for i := range b[1:] {
			b[1+i] = ^b[1+i]
		}
	}
}
