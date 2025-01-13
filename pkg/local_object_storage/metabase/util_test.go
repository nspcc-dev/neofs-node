package meta

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestNonUTF8Delimiter(t *testing.T) {
	t.Run("len", func(t *testing.T) { require.Len(t, utf8Delimiter, utf8DelimiterLen) })
	t.Run("format", func(t *testing.T) { require.False(t, utf8.Valid(utf8Delimiter)) })
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
