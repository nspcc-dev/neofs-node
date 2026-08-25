package object

import (
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestFormatAttributeValue(t *testing.T) {
	t.Run("timestamp interpreted by default", func(t *testing.T) {
		const ts = "1692609422"
		expected := fmt.Sprintf("%s (%s)", ts, common.PrettyPrintUnixTime(ts))
		require.Equal(t, expected, common.FormatAttributeValue(object.AttributeTimestamp, ts, false))
	})

	t.Run("timestamp raw output", func(t *testing.T) {
		const ts = "1692609422"
		require.Equal(t, ts, common.FormatAttributeValue(object.AttributeTimestamp, ts, true))
	})

	t.Run("other attribute always raw", func(t *testing.T) {
		require.Equal(t, "v", common.FormatAttributeValue("k", "v", false))
		require.Equal(t, "v", common.FormatAttributeValue("k", "v", true))
	})
}

func TestFormatAttribute(t *testing.T) {
	t.Run("default mode keeps key and interprets timestamp", func(t *testing.T) {
		const ts = "1692609422"
		key, val := common.FormatAttribute(object.AttributeTimestamp, ts, false)
		require.Equal(t, object.AttributeTimestamp, key)
		require.Equal(t, fmt.Sprintf("%s (%s)", ts, common.PrettyPrintUnixTime(ts)), val)
	})

	t.Run("raw mode quotes key and value", func(t *testing.T) {
		key, val := common.FormatAttribute("key\t", "value\n", true)
		require.Equal(t, `"key\t"`, key)
		require.Equal(t, `"value\n"`, val)
	})
}
