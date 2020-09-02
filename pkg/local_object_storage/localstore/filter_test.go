package localstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterResult(t *testing.T) {
	var (
		r *FilterResult
		c = CodePass
		e = errors.New("test error")
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
