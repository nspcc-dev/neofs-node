package meta

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/stretchr/testify/require"
)

type mockContainers struct {
	absent bool
	err    error
}

func (x mockContainers) Exists(cid.ID) (bool, error) { return !x.absent, x.err }

func TestAttributeDelimiter(t *testing.T) {
	t.Run("len", func(t *testing.T) { require.Len(t, objectcore.MetaAttributeDelimiter, attributeDelimiterLen) })
}

func TestKeyBuffer(t *testing.T) {
	var b keyBuffer
	b1 := b.alloc(10)
	require.Len(t, b1, 10)
	b2 := b.alloc(20)
	require.Len(t, b2, 20)
	b1 = b.alloc(10)
	require.Len(t, b1, 10)
	require.Equal(t, &b2[0], &b1[0])
}
