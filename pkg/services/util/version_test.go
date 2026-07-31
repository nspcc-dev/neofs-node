package util

import (
	"testing"

	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestNeedVersionInResponse(t *testing.T) {
	var curr = version.Current()

	tcs := []struct {
		name     string
		in       *protorefs.Version
		expected bool
	}{
		{
			"nil version",
			nil,
			true,
		},
		{
			"zero version",
			&protorefs.Version{Major: 0, Minor: 0},
			false,
		},
		{
			"v1, lower minor",
			&protorefs.Version{Major: 1, Minor: curr.Minor() - 1},
			false,
		},
		{
			"v1, equal minor",
			&protorefs.Version{Major: 1, Minor: curr.Minor()},
			false,
		},
		{
			"v1, higher minor",
			&protorefs.Version{Major: 1, Minor: curr.Minor() + 1},
			false,
		},
		{
			"curr major, lower minor",
			&protorefs.Version{Major: curr.Major(), Minor: curr.Minor() - 1},
			false,
		},
		{
			"equal",
			&protorefs.Version{Major: curr.Major(), Minor: curr.Minor()},
			false,
		},
		{
			"curr major, higher minor",
			&protorefs.Version{Major: curr.Major(), Minor: curr.Minor() + 1},
			true,
		},
		{
			"higher major, lower minor",
			&protorefs.Version{Major: curr.Major() + 1, Minor: curr.Minor() - 1},
			true,
		},
		{
			"higher major, equal minor",
			&protorefs.Version{Major: curr.Major() + 1, Minor: curr.Minor()},
			true,
		},
		{
			"higher major, higher minor",
			&protorefs.Version{Major: curr.Major() + 1, Minor: curr.Minor() + 1},
			true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			res := NeedVersionInResponse(&protosession.RequestMetaHeader{
				Version: tc.in,
			})
			require.Equal(t, tc.expected, res)
		})
	}
}
