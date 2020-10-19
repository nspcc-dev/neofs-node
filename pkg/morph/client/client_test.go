package client

import (
	"testing"

	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/stretchr/testify/require"
)

func TestToStackParameter(t *testing.T) {
	items := []struct {
		value   interface{}
		expType sc.ParamType
	}{
		{
			value:   []byte{1, 2, 3},
			expType: sc.ByteArrayType,
		},
		{
			value:   int64(100),
			expType: sc.IntegerType,
		},
		{
			value:   "hello world",
			expType: sc.StringType,
		},
	}

	for _, item := range items {
		t.Run(item.expType.String()+" to stack parameter", func(t *testing.T) {
			res, err := toStackParameter(item.value)
			require.NoError(t, err)
			require.Equal(t, item.expType, res.Type)
			require.Equal(t, item.value, res.Value)
		})
	}
}
