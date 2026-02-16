package meta

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Search selects up to count container's objects from the given container
// matching the specified filters.
func (m *Meta) Search(cID cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	m.stM.RLock()
	s, ok := m.storages[cID]
	m.stM.RUnlock()

	if !ok {
		m.l.Debug("skip search request due inactual container", zap.Stringer("cid", cID))
		return nil, nil, nil
	}

	return s.search(fs, attrs, cursor, count)
}

func (s *containerStorage) search(fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	s.m.Lock()
	defer s.m.Unlock()

	if len(fs) == 0 {
		return searchUnfiltered(s.db, cursor, count)
	}

	resHolder := objectcore.SearchResult{Objects: make([]client.SearchResultItem, 0, count)}
	handleKV := objectcore.MetaDataKVHandler(&resHolder, &attrGetter{db: s.db}, nil, fs, attrs, cursor, count)

	var rng storage.SeekRange
	if cursor != nil {
		rng.Prefix = cursor.PrimaryKeysPrefix
		rng.Start = bytes.TrimPrefix(cursor.PrimarySeekKey, cursor.PrimaryKeysPrefix)
	}
	s.db.Seek(rng, func(k, v []byte) bool {
		if cursor != nil && bytes.Equal(k, cursor.PrimarySeekKey) {
			// points to the last response element, so go next
			return true
		}

		return handleKV(k, v)
	})

	return resHolder.Objects, resHolder.UpdatedSearchCursor, resHolder.Err
}

// lock must be taken.
func searchUnfiltered(st storage.Store, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	res := make([]client.SearchResultItem, 0, count)
	var err error
	var newCursor []byte

	var rng storage.SeekRange
	if cursor != nil {
		rng.Prefix = cursor.PrimaryKeysPrefix
		rng.Start = bytes.TrimPrefix(cursor.PrimarySeekKey, cursor.PrimaryKeysPrefix)
	} else {
		rng.Prefix = []byte{oidIndex}
	}

	st.Seek(rng, func(k, _ []byte) bool {
		if bytes.Equal(k, cursor.PrimarySeekKey) {
			return true
		}
		if len(res) == int(count) {
			newCursor = res[len(res)-1].ID[:]
			return false
		}

		if len(k) != oid.Size+1 {
			err = fmt.Errorf("invalid meta bucket key (prefix 0x%X): unexpected object key len %d", k[0], len(k))
			return false
		}
		res = append(res, client.SearchResultItem{ID: oid.ID(k[1:])})

		return true
	})

	return res, newCursor, err
}

type attrGetter struct {
	keyBuff []byte

	db storage.Store
}

func (a *attrGetter) Get(oID []byte, attributeKey string) (attributeValue []byte, err error) {
	if len(a.keyBuff) > 0 {
		a.keyBuff = a.keyBuff[:0]
	}

	a.keyBuff = slices.Grow(a.keyBuff, attrIDFixedLen+len(attributeKey))
	a.keyBuff = append(a.keyBuff, oidToAttrIndex)
	a.keyBuff = append(a.keyBuff, oID...)
	a.keyBuff = append(a.keyBuff, attributeKey...)
	a.keyBuff = append(a.keyBuff, objectcore.MetaAttributeDelimiter...)

	var rng storage.SeekRange
	rng.Start = a.keyBuff
	a.db.Seek(rng, func(k, v []byte) bool {
		if !bytes.HasPrefix(k, a.keyBuff) {
			return false
		}
		if len(k[len(a.keyBuff):]) == 0 {
			err = fmt.Errorf("invalid meta bucket key (prefix 0x%X): missing attribute value", k[0])
			return false
		}
		attributeValue = slices.Clone(k[len(a.keyBuff):])

		return false
	})

	return
}
