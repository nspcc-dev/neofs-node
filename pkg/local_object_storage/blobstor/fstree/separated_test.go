package fstree

import (
	"testing"

	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestSeparatedObject(t *testing.T) {
	obj := objecttest.Object()
	obj.SetPayload([]byte("payload"))
	canonical := obj.Marshal()

	separated := separateObject(canonical)
	headerLen, payloadLen := parseSeparatedPrefix(separated)
	require.NotZero(t, headerLen)
	require.EqualValues(t, len(obj.Payload()), payloadLen)

	restored, ok, err := restoreSeparatedObject(separated)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, canonical, restored)
}
