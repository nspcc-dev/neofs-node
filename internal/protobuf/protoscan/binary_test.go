package protoscan_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	"github.com/stretchr/testify/require"
)

func TestBinaryKind_String(t *testing.T) {
	for val, str := range map[protoscan.BinaryFieldKind]string{
		0:                                    "unknown#0",
		protoscan.BinaryFieldKindSHA256:      "SHA256",
		protoscan.BinaryFieldKindNeo3Address: "Neo3Address",
		protoscan.BinaryFieldKindUUIDV4:      "UUIDV4",
		4:                                    "unknown#4",
	} {
		t.Run(str, func(t *testing.T) {
			require.Equal(t, str, val.String())
		})
	}
}
