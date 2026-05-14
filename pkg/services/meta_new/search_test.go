package meta

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/services/sidechain"
	metatest "github.com/nspcc-dev/neofs-node/pkg/util/meta/test"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type singleObjectMeta struct {
	*Meta
}

type noopNetwork struct{}

func (n noopNetwork) IsMineWithMeta(id cid.ID, bytes []byte) (bool, error) {
	panic("do not call me")
}

func (n noopNetwork) Head(ctx context.Context, cID cid.ID, oID oid.ID) (object.Object, error) {
	panic("do not call me")
}

func (n *singleObjectMeta) Put(obj *object.Object) error {
	return n.PutObjects([]*object.Object{obj})
}

func TestMeta_Search(t *testing.T) {
	m, err := New(Parameters{
		Logger:  zaptest.NewLogger(t),
		Chain:   &sidechain.SideChain{},
		Path:    t.TempDir(),
		Network: noopNetwork{},
	})
	require.NoError(t, err)

	metatest.TestSearchObjects(t, &singleObjectMeta{m}, false)
}
