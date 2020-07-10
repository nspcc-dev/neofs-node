package localstore

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/stretchr/testify/require"
)

func TestSkippingFilterFunc(t *testing.T) {
	res := SkippingFilterFunc(context.TODO(), &ObjectMeta{})
	require.Equal(t, CodePass, res.Code())
}

func TestFilterResult(t *testing.T) {
	var (
		r *FilterResult
		c = CodePass
		e = internal.Error("test error")
	)

	r = ResultPass()
	require.Equal(t, CodePass, r.Code())
	require.NoError(t, r.Err())

	r = ResultFail()
	require.Equal(t, CodeFail, r.Code())
	require.NoError(t, r.Err())

	r = ResultIgnore()
	require.Equal(t, CodeIgnore, r.Code())
	require.NoError(t, r.Err())

	r = ResultWithError(c, e)
	require.Equal(t, c, r.Code())
	require.EqualError(t, r.Err(), e.Error())
}
