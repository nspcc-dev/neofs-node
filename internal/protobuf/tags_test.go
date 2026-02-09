package protobuf_test

import (
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestTags(t *testing.T) {
	t.Run("LEN", func(t *testing.T) {
		for _, tc := range []struct {
			tag byte
			num int
		}{
			{tag: iprotobuf.TagBytes1, num: 1},
			{tag: iprotobuf.TagBytes2, num: 2},
			{tag: iprotobuf.TagBytes3, num: 3},
			{tag: iprotobuf.TagBytes4, num: 4},
			{tag: iprotobuf.TagBytes5, num: 5},
			{tag: iprotobuf.TagBytes6, num: 6},
		} {
			require.EqualValues(t, protowire.EncodeTag(protowire.Number(tc.num), protowire.BytesType), tc.tag)

			num, typ, n, err := iprotobuf.ParseTag([]byte{tc.tag})
			require.NoError(t, err)
			require.EqualValues(t, 1, n)
			require.EqualValues(t, protowire.BytesType, typ)
			require.EqualValues(t, tc.num, num)
		}
	})

	t.Run("VARINT", func(t *testing.T) {
		for _, tc := range []struct {
			tag byte
			num int
		}{
			{tag: iprotobuf.TagVarint1, num: 1},
			{tag: iprotobuf.TagVarint2, num: 2},
			{tag: iprotobuf.TagVarint3, num: 3},
			{tag: iprotobuf.TagVarint4, num: 4},
			{tag: iprotobuf.TagVarint5, num: 5},
			{tag: iprotobuf.TagVarint6, num: 6},
		} {
			require.EqualValues(t, protowire.EncodeTag(protowire.Number(tc.num), protowire.VarintType), tc.tag)

			num, typ, n, err := iprotobuf.ParseTag([]byte{tc.tag})
			require.NoError(t, err)
			require.EqualValues(t, 1, n)
			require.EqualValues(t, protowire.VarintType, typ)
			require.EqualValues(t, tc.num, num)
		}
	})
}
