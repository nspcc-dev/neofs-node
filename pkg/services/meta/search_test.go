package meta

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	metatest "github.com/nspcc-dev/neofs-node/pkg/util/meta/test"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type searchTestDB struct {
	t        *testing.T
	storages map[cid.ID]*containerStorage
	l        *zap.Logger
}

func (s searchTestDB) Put(obj *object.Object) error {
	cID := obj.GetContainerID()
	st, ok := s.storages[cID]
	if !ok {
		var err error

		st, err = storageForContainer(s.l, s.t.TempDir(), cID)
		require.NoError(s.t, err)
		s.t.Cleanup(func() {
			_ = st.drop()
		})

		s.storages[cID] = st
	}

	batch := make(map[string][]byte)
	fillObjectIndex(batch, *obj, false)
	return st.db.PutChangeSet(batch, nil)
}

func (s searchTestDB) Search(cnr cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	return s.storages[cnr].search(fs, attrs, cursor, count)
}

func TestMeta_Search(t *testing.T) {
	db := searchTestDB{t: t, l: zaptest.NewLogger(t), storages: make(map[cid.ID]*containerStorage)}
	metatest.TestSearchObjects(t, db, false)
}
