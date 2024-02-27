package pilorama

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMeta_Bytes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var m Meta
		require.NoError(t, m.FromBytes(nil))
		require.True(t, len(m.Items) == 0)
		require.Equal(t, uint64(0), m.Time)
		require.Equal(t, []byte{0, 0}, m.Bytes())
	})
	t.Run("empty attribute value", func(t *testing.T) {
		expected := Meta{
			Time: 123,
			Items: []KeyValue{
				{"abc", []byte{1, 2, 3}},
				{AttributeFilename, []byte{}},
			}}

		data := expected.Bytes()

		var actual Meta
		require.NoError(t, actual.FromBytes(data))
		require.Equal(t, expected, actual)
	})
	t.Run("filled", func(t *testing.T) {
		expected := Meta{
			Time: 123,
			Items: []KeyValue{
				{"abc", []byte{1, 2, 3}},
				{"xyz", []byte{5, 6, 7, 8}},
			}}

		data := expected.Bytes()

		var actual Meta
		require.NoError(t, actual.FromBytes(data))
		require.Equal(t, expected, actual)

		t.Run("error", func(t *testing.T) {
			require.Error(t, new(Meta).FromBytes(data[:len(data)/2]))
		})
	})
}

func TestMeta_GetAttr(t *testing.T) {
	attr := [][]byte{
		make([]byte, 5),
		make([]byte, 10),
	}
	for i := range attr {
		//nolint:staticcheck
		rand.Read(attr[i])
	}

	m := Meta{Items: []KeyValue{{"abc", attr[0]}, {"xyz", attr[1]}}}
	require.Equal(t, attr[0], m.GetAttr("abc"))
	require.Equal(t, attr[1], m.GetAttr("xyz"))

	require.Nil(t, m.GetAttr("a"))
	require.Nil(t, m.GetAttr("xyza"))
	require.Nil(t, m.GetAttr(""))
}
