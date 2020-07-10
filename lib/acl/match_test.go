package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/stretchr/testify/require"
)

type testTypedHeader struct {
	t acl.HeaderType
	k string
	v string
}

type testHeaderSrc struct {
	hs []acl.TypedHeader
}

type testHeaderFilter struct {
	acl.TypedHeader
	t acl.MatchType
}

func (s testHeaderFilter) MatchType() acl.MatchType {
	return s.t
}

func (s testHeaderSrc) HeadersOfType(typ acl.HeaderType) ([]acl.Header, bool) {
	res := make([]acl.Header, 0, len(s.hs))

	for i := range s.hs {
		if s.hs[i].HeaderType() == typ {
			res = append(res, s.hs[i])
		}
	}

	return res, true
}

func (s testTypedHeader) Name() string {
	return s.k
}

func (s testTypedHeader) Value() string {
	return s.v
}

func (s testTypedHeader) HeaderType() acl.HeaderType {
	return s.t
}

func TestMatchFilters(t *testing.T) {
	// nil TypedHeaderSource
	require.Equal(t, mResMismatch, MatchFilters(nil, nil))

	// empty HeaderFilter list
	require.Equal(t, mResMatch, MatchFilters(new(testHeaderSrc), nil))

	k := "key"
	v := "value"
	ht := acl.HeaderType(1)

	items := []struct {
		// list of Key-Value-HeaderType for headers construction
		hs []interface{}
		// list of Key-Value-HeaderType-MatchType for filters construction
		fs  []interface{}
		exp int
	}{
		{ // different HeaderType
			hs: []interface{}{
				k, v, ht,
			},
			fs: []interface{}{
				k, v, ht + 1, acl.StringEqual,
			},
			exp: mResMismatch,
		},
		{ // different keys
			hs: []interface{}{
				k, v, ht,
			},
			fs: []interface{}{
				k + "1", v, ht, acl.StringEqual,
			},
			exp: mResMismatch,
		},
		{ // equal values, StringEqual
			hs: []interface{}{
				k, v, ht,
			},
			fs: []interface{}{
				k, v, ht, acl.StringEqual,
			},
			exp: mResMatch,
		},
		{ // equal values, StringNotEqual
			hs: []interface{}{
				k, v, ht,
			},
			fs: []interface{}{
				k, v, ht, acl.StringNotEqual,
			},
			exp: mResMismatch,
		},
		{ // not equal values, StringEqual
			hs: []interface{}{
				k, v, ht,
			},
			fs: []interface{}{
				k, v + "1", ht, acl.StringEqual,
			},
			exp: mResMismatch,
		},
		{ // not equal values, StringNotEqual
			hs: []interface{}{
				k, v, ht,
			},
			fs: []interface{}{
				k, v + "1", ht, acl.StringNotEqual,
			},
			exp: mResMatch,
		},
		{ // one header, two filters
			hs: []interface{}{
				k, v, ht,
			},
			fs: []interface{}{
				k, v + "1", ht, acl.StringNotEqual,
				k, v, ht, acl.StringEqual,
			},
			exp: mResMatch,
		},
		{ // two headers, one filter
			hs: []interface{}{
				k, v + "1", ht,
				k, v, ht,
			},
			fs: []interface{}{
				k, v, ht, acl.StringEqual,
			},
			exp: mResMatch,
		},
		{
			hs: []interface{}{
				k, v + "1", acl.HdrTypeRequest,
				k, v, acl.HdrTypeObjUsr,
			},
			fs: []interface{}{
				k, v, acl.HdrTypeRequest, acl.StringNotEqual,
				k, v, acl.HdrTypeObjUsr, acl.StringEqual,
			},
			exp: mResMatch,
		},
	}

	for _, item := range items {
		headers := make([]acl.TypedHeader, 0)

		for i := 0; i < len(item.hs); i += 3 {
			headers = append(headers, &testTypedHeader{
				t: item.hs[i+2].(acl.HeaderType),
				k: item.hs[i].(string),
				v: item.hs[i+1].(string),
			})
		}

		filters := make([]acl.HeaderFilter, 0)

		for i := 0; i < len(item.fs); i += 4 {
			filters = append(filters, &testHeaderFilter{
				TypedHeader: &testTypedHeader{
					t: item.fs[i+2].(acl.HeaderType),
					k: item.fs[i].(string),
					v: item.fs[i+1].(string),
				},
				t: item.fs[i+3].(acl.MatchType),
			})
		}

		require.Equal(t,
			item.exp,
			MatchFilters(
				&testHeaderSrc{
					hs: headers,
				},
				filters,
			),
		)
	}
}
