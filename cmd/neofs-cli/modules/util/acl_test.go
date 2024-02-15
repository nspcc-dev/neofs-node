package util

import (
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/stretchr/testify/require"
)

func TestParseKVWithOp(t *testing.T) {
	for _, tc := range []struct {
		s  string
		k  string
		op eacl.Match
		v  string
	}{
		{"=", "", eacl.MatchNotPresent, ""},
		{"!=", "!", eacl.MatchNotPresent, ""},
		{">1234567890", "", eacl.MatchNumGT, "1234567890"},
		{"<1234567890", "", eacl.MatchNumLT, "1234567890"},
		{">=1234567890", "", eacl.MatchNumGE, "1234567890"},
		{"=>", "", eacl.MatchStringEqual, ">"},
		{"=<", "", eacl.MatchStringEqual, "<"},
		{"key=", "key", eacl.MatchNotPresent, ""},
		{"key>=", "key>", eacl.MatchNotPresent, ""},
		{"key<=", "key<", eacl.MatchNotPresent, ""},
		{"=value", "", eacl.MatchStringEqual, "value"},
		{"!=value", "", eacl.MatchStringNotEqual, "value"},
		{"key=value", "key", eacl.MatchStringEqual, "value"},
		{"key>1234567890", "key", eacl.MatchNumGT, "1234567890"},
		{"key<1234567890", "key", eacl.MatchNumLT, "1234567890"},
		{"key==value", "key", eacl.MatchStringEqual, "=value"},
		{"key=>value", "key", eacl.MatchStringEqual, ">value"},
		{"key>=1234567890", "key", eacl.MatchNumGE, "1234567890"},
		{"key<=1234567890", "key", eacl.MatchNumLE, "1234567890"},
		{"key=<value", "key", eacl.MatchStringEqual, "<value"},
		{"key!=value", "key", eacl.MatchStringNotEqual, "value"},
		{"key=!value", "key", eacl.MatchStringEqual, "!value"},
		{"key!=!value", "key", eacl.MatchStringNotEqual, "!value"},
		{"key!!=value", "key!", eacl.MatchStringNotEqual, "value"},
		{"key!=!=value", "key", eacl.MatchStringNotEqual, "!=value"},
		{"key=value=42", "key", eacl.MatchStringEqual, "value=42"},
		{"key!=value!=42", "key", eacl.MatchStringNotEqual, "value!=42"},
		{"k e y = v a l u e", "k e y ", eacl.MatchStringEqual, " v a l u e"},
		{"k e y != v a l u e", "k e y ", eacl.MatchStringNotEqual, " v a l u e"},
	} {
		k, v, op, err := parseKVWithOp(tc.s)
		require.NoError(t, err, tc)
		require.Equal(t, tc.k, k, tc)
		require.Equal(t, tc.v, v, tc)
		require.Equal(t, tc.op, op, tc)
	}

	for _, tc := range []struct {
		s string
		e string
	}{
		{"", "missing op"},
		{"!", "missing op"},
		{">", "invalid base-10 integer value \"\" for attribute \"\""},
		{">1.2", "invalid base-10 integer value \"1.2\" for attribute \"\""},
		{">=1.2", "invalid base-10 integer value \"1.2\" for attribute \"\""},
		{"<", "invalid base-10 integer value \"\" for attribute \"\""},
		{"<1.2", "invalid base-10 integer value \"1.2\" for attribute \"\""},
		{"<=1.2", "invalid base-10 integer value \"1.2\" for attribute \"\""},
		{"k", "missing op"},
		{"k!", "missing op"},
		{"k>", "invalid base-10 integer value \"\" for attribute \"k\""},
		{"k<", "invalid base-10 integer value \"\" for attribute \"k\""},
		{"k!v", "missing op"},
		{"k<v", "invalid base-10 integer value \"v\" for attribute \"k\""},
		{"k<=v", "invalid base-10 integer value \"v\" for attribute \"k\""},
		{"k>=v", "invalid base-10 integer value \"v\" for attribute \"k\""},
	} {
		_, _, _, err := parseKVWithOp(tc.s)
		require.ErrorContains(t, err, tc.e, tc)
	}
}

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

		err := ValidateEACLTable(&tb)
		require.ErrorContains(t, err, "non-empty value in absence filter")

		r = eacl.Record{}
		r.AddObjectAttributeFilter(eacl.MatchNotPresent, "any_key", "")
		tb = anyValidEACL()
		tb.AddRecord(&r)

		err = ValidateEACLTable(&tb)
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

				err := ValidateEACLTable(&tb)
				if tc.ok {
					require.NoError(t, err, [2]any{m, tc})
				} else {
					require.ErrorContains(t, err, "numeric filter with non-decimal value", [2]any{m, tc})
				}
			}
		}
	})
}
