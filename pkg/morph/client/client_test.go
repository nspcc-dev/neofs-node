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
		expVal  interface{}
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
		{
			value:   false,
			expType: sc.IntegerType,
			expVal:  int64(0),
		},
		{
			value:   true,
			expType: sc.IntegerType,
			expVal:  int64(1),
		},
	}

	for _, item := range items {
		t.Run(item.expType.String()+" to stack parameter", func(t *testing.T) {
			res, err := toStackParameter(item.value)
			require.NoError(t, err)
			require.Equal(t, item.expType, res.Type)
			if item.expVal != nil {
				require.Equal(t, item.expVal, res.Value)
			} else {
				require.Equal(t, item.value, res.Value)
			}
		})
	}
}
