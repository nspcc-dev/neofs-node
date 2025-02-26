package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"golang.org/x/sync/errgroup"
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

func (s *containerStorage) putMPTIndexes(ee []objEvent) {
	s.m.Lock()
	defer s.m.Unlock()

	for _, e := range ee {
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
}

func (s *containerStorage) putRawIndexes(ctx context.Context, ee []objEvent, net NeoFSNetwork) error {
	var wg errgroup.Group
	wg.SetLimit(10)
	objects := make([]objectsdk.Object, len(ee))

	for i, e := range ee {
		wg.Go(func() error {
			h, err := net.Head(ctx, e.cID, e.oID)
			if err != nil {
				// TODO define behavior with status (non-network) errors; maybe it is near #3140
				return fmt.Errorf("HEAD object: %w", err)
			}

			objects[i] = h
			return nil
		})
	}

	err := wg.Wait()
	if err != nil {
		return err
	}

	s.m.Lock()
	defer s.m.Unlock()
	batch := make(map[string][]byte)

	for i, e := range ee {
		commsuffix := e.oID[:]

		batch[string(append([]byte{oidIndex}, commsuffix...))] = []byte{}
		if len(e.deletedObjects) > 0 {
			batch[string(append([]byte{deletedIndex}, commsuffix...))] = e.deletedObjects
			err = deleteObjectsOps(batch, s.mptOpsBatch, s.db, e.deletedObjects)
			if err != nil {
				return fmt.Errorf("cleaning operations for %s object: %w", e.oID, err)
			}
		}
		if len(e.lockedObjects) > 0 {
			batch[string(append([]byte{lockedIndex}, commsuffix...))] = e.lockedObjects
		}

		err = validateHeader(objects[i])
		if err != nil {
			return fmt.Errorf("invalid %s header: %w", e.oID, err)
		}

		fillObjectIndex(batch, objects[i])
	}

	err = s.db.PutChangeSet(batch, nil)
	if err != nil {
		return fmt.Errorf("put change set to DB: %w", err)
	}

	return nil
}

func validateHeader(h objectsdk.Object) error {
	if h.Owner().IsZero() {
		return fmt.Errorf("invalid owner: %w", user.ErrZeroID)
	}
	_, ok := h.PayloadChecksum()
	if !ok {
		return errors.New("missing payload checksum")
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

func deleteObjectsOps(dbKV, mptKV map[string][]byte, s storage.Store, objects []byte) error {
	rng := storage.SeekRange{}

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
			// common keys for DB and MTP storages
			case oidIndex, deletedIndex, lockedIndex:
				mptKV[string(append([]byte{0}, k...))] = nil
			// DB reversed indexes
			case oidToAttrIndex:
				withoutOID := k[1+oid.Size:]
				i := bytes.Index(withoutOID, attributeDelimiter)
				if i < 0 {
					err = fmt.Errorf("unexpected attribute index without delimeter: %s", string(k))
					return false
				}
				attrK := withoutOID[:i]
				attrV := withoutOID[i+attributeDelimiterLen:]

				// drop reverse plain index
				keyToDrop := make([]byte, 0, len(k)+len(attributeDelimiter))
				keyToDrop = append(keyToDrop, attrPlainToOIDIndex)
				keyToDrop = append(keyToDrop, withoutOID...)
				keyToDrop = append(keyToDrop, attributeDelimiter...)
				keyToDrop = append(keyToDrop, o...)

				dbKV[string(keyToDrop)] = nil

				if vInt, isInt := parseInt(string(attrV)); isInt && intWithinLimits(vInt) {
					keyToDrop = slices.Grow(keyToDrop, 1+len(attrK)+attributeDelimiterLen+intValLen+oid.Size)
					// drop reverse int index
					keyToDrop = keyToDrop[:0]
					keyToDrop = append(keyToDrop, attrIntToOIDIndex)
					keyToDrop = append(keyToDrop, attrK...)
					keyToDrop = append(keyToDrop, attributeDelimiter...)
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
			return err
		}
	}

	return nil
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

// object attribute key and value separator used in DB.
var attributeDelimiter = []byte{0x00}

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
	resKey = append(resKey, attributeDelimiter...)
	resKey = append(resKey, v...)

	batch[string(resKey)] = []byte{}
	resKey = resKey[:0]

	// PREFIX_ATTR_DELIM_VAL_DELIM_OID
	resKey = append(resKey, attrPlainToOIDIndex)
	resKey = append(resKey, k...)
	resKey = append(resKey, attributeDelimiter...)
	resKey = append(resKey, v...)
	resKey = append(resKey, attributeDelimiter...)
	resKey = append(resKey, id[:]...)

	batch[string(resKey)] = []byte{}
}

func putIntAttribute(batch map[string][]byte, id oid.ID, k, vRaw string, vParsed *big.Int) {
	putPlainAttribute(batch, id, k, vRaw)

	resKey := make([]byte, 0, 1+len(k)+attributeDelimiterLen+intValLen+oid.Size)

	// PREFIX_ATTR_DELIM_VAL_OID
	resKey = append(resKey, attrIntToOIDIndex)
	resKey = append(resKey, k...)
	resKey = append(resKey, attributeDelimiter...)

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
