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
