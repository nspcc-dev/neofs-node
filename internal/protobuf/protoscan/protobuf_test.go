package protoscan_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	"github.com/stretchr/testify/require"
)

func TestFieldType_String(t *testing.T) {
	for val, str := range map[protoscan.FieldType]string{
		0:                                "unknown#0",
		protoscan.FieldTypeEnum:          "enum",
		protoscan.FieldTypeUint32:        "uint32",
		protoscan.FieldTypeUint64:        "uint64",
		protoscan.FieldTypeBool:          "bool",
		protoscan.FieldTypeBytes:         "bytes",
		protoscan.FieldTypeString:        "string",
		protoscan.FieldTypeRepeatedEnum:  "repeated enum",
		protoscan.FieldTypeNestedMessage: "nested message",
		9:                                "unknown#9",
	} {
		t.Run(str, func(t *testing.T) {
			require.Equal(t, str, val.String())
		})
	}
}
