package object

import (
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestFormatAttribute(t *testing.T) {
	t.Run("default mode keeps key and interprets timestamp", func(t *testing.T) {
		const ts = "1692609422"
		key, val := common.FormatAttribute(object.AttributeTimestamp, ts, false)
		require.Equal(t, object.AttributeTimestamp, key)
		require.Equal(t, fmt.Sprintf("%s (%s)", ts, common.PrettyPrintUnixTime(ts)), val)
	})

	t.Run("default mode keeps other attributes as-is", func(t *testing.T) {
		key, val := common.FormatAttribute("k", "v", false)
		require.Equal(t, "k", key)
		require.Equal(t, "v", val)
	})

	t.Run("raw mode quotes key and value", func(t *testing.T) {
		key, val := common.FormatAttribute("key\t", "value\n", true)
		require.Equal(t, `"key\t"`, key)
		require.Equal(t, `"value\n"`, val)
	})

	t.Run("raw mode does not interpret timestamp", func(t *testing.T) {
		key, val := common.FormatAttribute(object.AttributeTimestamp, "1692609422", true)
		require.Equal(t, `"`+object.AttributeTimestamp+`"`, key)
		require.Equal(t, `"1692609422"`, val)
	})
}
