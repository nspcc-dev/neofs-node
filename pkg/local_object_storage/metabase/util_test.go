package meta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAttributeDelimiter(t *testing.T) {
	t.Run("len", func(t *testing.T) { require.Len(t, attributeDelimiter, attributeDelimiterLen) })
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
