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
		{"=", "", eacl.MatchStringEqual, ""},
		{"!=", "", eacl.MatchStringNotEqual, ""},
		{">=", ">", eacl.MatchStringEqual, ""},
		{"=>", "", eacl.MatchStringEqual, ">"},
		{"<=", "<", eacl.MatchStringEqual, ""},
		{"=<", "", eacl.MatchStringEqual, "<"},
		{"key=", "key", eacl.MatchStringEqual, ""},
		{"key>=", "key>", eacl.MatchStringEqual, ""},
		{"key<=", "key<", eacl.MatchStringEqual, ""},
		{"=value", "", eacl.MatchStringEqual, "value"},
		{"!=value", "", eacl.MatchStringNotEqual, "value"},
		{"key=value", "key", eacl.MatchStringEqual, "value"},
		{"key==value", "key", eacl.MatchStringEqual, "=value"},
		{"key=>value", "key", eacl.MatchStringEqual, ">value"},
		{"key>=value", "key>", eacl.MatchStringEqual, "value"},
		{"key<=value", "key<", eacl.MatchStringEqual, "value"},
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
		{">", "missing op"},
		{"<", "missing op"},
		{"k", "missing op"},
		{"k!", "missing op"},
		{"k>", "missing op"},
		{"k<", "missing op"},
		{"k!v", "missing op"},
		{"k<v", "missing op"},
		{"k>v", "missing op"},
	} {
		_, _, _, err := parseKVWithOp(tc.s)
		require.ErrorContains(t, err, tc.e, tc)
	}
}
