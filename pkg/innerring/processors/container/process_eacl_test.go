package container

import (
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/stretchr/testify/require"
)

var allNumMatchers = []eacl.Match{eacl.MatchNumGT, eacl.MatchNumGE, eacl.MatchNumLT, eacl.MatchNumLE}

func anyValidEACL() eacl.Table {
	return eacl.Table{}
}

func TestValidateEACL(t *testing.T) {
	t.Run("absence matcher", func(t *testing.T) {
		var r eacl.Record
		r.AddObjectAttributeFilter(eacl.MatchNotPresent, "any_key", "any_value")
		tb := anyValidEACL()
		tb.AddRecord(&r)

		err := validateEACL(&tb)
		require.ErrorContains(t, err, "non-empty value in absence filter")

		r = eacl.Record{}
		r.AddObjectAttributeFilter(eacl.MatchNotPresent, "any_key", "")
		tb = anyValidEACL()
		tb.AddRecord(&r)

		err = validateEACL(&tb)
		require.NoError(t, err)
	})

	t.Run("numeric matchers", func(t *testing.T) {
		for _, tc := range []struct {
			ok bool
			v  string
		}{
			{false, "not a base-10 integer"},
			{false, "1.2"},
			{false, ""},
			{true, "01"},
			{true, "0"},
			{true, "01"},
			{true, "-0"},
			{true, "-01"},
			{true, "1111111111111111111111111111111111111111111111"},
			{true, "-1111111111111111111111111111111111111111111111"},
		} {
			for _, m := range allNumMatchers {
				var r eacl.Record
				r.AddObjectAttributeFilter(m, "any_key", tc.v)
				tb := anyValidEACL()
				tb.AddRecord(&r)

				err := validateEACL(&tb)
				if tc.ok {
					require.NoError(t, err, [2]any{m, tc})
				} else {
					require.ErrorContains(t, err, "numeric filter with non-decimal value", [2]any{m, tc})
				}
			}
		}
	})
}
