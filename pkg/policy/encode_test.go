package policy_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/policy"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	testCases := []string{
		`REP 1 IN X
CBF 1
SELECT 2 IN SAME Location FROM * AS X`,

		`REP 1
SELECT 2 IN City FROM Good
FILTER Country EQ RU AS FromRU
FILTER @FromRU AND Rating GT 7 AS Good`,

		`REP 7 IN SPB
SELECT 1 IN City FROM SPBSSD AS SPB
FILTER City EQ SPB AND SSD EQ true OR City EQ SPB AND Rating GE 5 AS SPBSSD`,
	}

	for _, testCase := range testCases {
		q, err := policy.Parse(testCase)
		require.NoError(t, err)

		got := policy.Encode(q)
		fmt.Println(strings.Join(got, "\n"))
		require.Equal(t, testCase, strings.Join(got, "\n"))
	}
}
