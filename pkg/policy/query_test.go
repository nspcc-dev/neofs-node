package policy

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	q := `REP 3`
	expected := new(netmap.PlacementPolicy)
	expected.SetFilters([]*netmap.Filter{})
	expected.SetSelectors([]*netmap.Selector{})
	expected.SetReplicas([]*netmap.Replica{newReplica("", 3)})

	r, err := Parse(q)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestSimpleWithHRWB(t *testing.T) {
	q := `REP 3 CBF 4`
	expected := new(netmap.PlacementPolicy)
	expected.SetFilters([]*netmap.Filter{})
	expected.SetSelectors([]*netmap.Selector{})
	expected.SetReplicas([]*netmap.Replica{newReplica("", 3)})
	expected.SetContainerBackupFactor(4)

	r, err := Parse(q)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestFromSelect(t *testing.T) {
	q := `REP 1 IN SPB
SELECT 1 IN City FROM * AS SPB`
	expected := new(netmap.PlacementPolicy)
	expected.SetFilters([]*netmap.Filter{})
	expected.SetSelectors([]*netmap.Selector{
		newSelector(1, netmap.UnspecifiedClause, "City", "*", "SPB"),
	})
	expected.SetReplicas([]*netmap.Replica{newReplica("SPB", 1)})

	r, err := Parse(q)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

// https://github.com/nspcc-dev/neofs-node/issues/46
func TestFromSelectNoAttribute(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		q := `REP 2
		SELECT 6 FROM *`

		expected := new(netmap.PlacementPolicy)
		expected.SetFilters([]*netmap.Filter{})
		expected.SetSelectors([]*netmap.Selector{newSelector(6, netmap.UnspecifiedClause, "", "*", "")})
		expected.SetReplicas([]*netmap.Replica{newReplica("", 2)})

		r, err := Parse(q)
		require.NoError(t, err)
		require.Equal(t, expected, r)

	})

	t.Run("with filter", func(t *testing.T) {
		q := `REP 2
		SELECT 6 FROM F
		FILTER StorageType EQ SSD AS F`

		expected := new(netmap.PlacementPolicy)
		expected.SetFilters([]*netmap.Filter{newFilter("F", "StorageType", "SSD", netmap.EQ)})
		expected.SetSelectors([]*netmap.Selector{newSelector(6, netmap.UnspecifiedClause, "", "F", "")})
		expected.SetReplicas([]*netmap.Replica{newReplica("", 2)})

		r, err := Parse(q)
		require.NoError(t, err)
		require.Equal(t, expected, r)
	})
}

func TestFromSelectClause(t *testing.T) {
	q := `REP 4
SELECT 3 IN Country FROM *
SELECT 2 IN SAME City FROM *
SELECT 1 IN DISTINCT Continent FROM *`
	expected := new(netmap.PlacementPolicy)
	expected.SetFilters([]*netmap.Filter{})
	expected.SetSelectors([]*netmap.Selector{
		newSelector(3, netmap.UnspecifiedClause, "Country", "*", ""),
		newSelector(2, netmap.Same, "City", "*", ""),
		newSelector(1, netmap.Distinct, "Continent", "*", ""),
	})
	expected.SetReplicas([]*netmap.Replica{newReplica("", 4)})

	r, err := Parse(q)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestSimpleFilter(t *testing.T) {
	q := `REP 1
SELECT 1 IN City FROM Good
FILTER Rating GT 7 AS Good`
	expected := new(netmap.PlacementPolicy)
	expected.SetReplicas([]*netmap.Replica{newReplica("", 1)})
	expected.SetSelectors([]*netmap.Selector{
		newSelector(1, netmap.UnspecifiedClause, "City", "Good", ""),
	})
	expected.SetFilters([]*netmap.Filter{
		newFilter("Good", "Rating", "7", netmap.GT),
	})

	r, err := Parse(q)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestFilterReference(t *testing.T) {
	q := `REP 1
SELECT 2 IN City FROM Good
FILTER Country EQ "RU" AS FromRU
FILTER @FromRU AND Rating GT 7 AS Good`
	expected := new(netmap.PlacementPolicy)
	expected.SetReplicas([]*netmap.Replica{newReplica("", 1)})
	expected.SetSelectors([]*netmap.Selector{
		newSelector(2, netmap.UnspecifiedClause, "City", "Good", ""),
	})
	expected.SetFilters([]*netmap.Filter{
		newFilter("FromRU", "Country", "RU", netmap.EQ),
		newFilter("Good", "", "", netmap.AND,
			newFilter("FromRU", "", "", netmap.UnspecifiedOperation),
			newFilter("", "Rating", "7", netmap.GT)),
	})

	r, err := Parse(q)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestFilterOps(t *testing.T) {
	q := `REP 1
SELECT 2 IN City FROM Good
FILTER A GT 1 AND B GE 2 AND C LT 3 AND D LE 4
  AND E EQ 5 AND F NE 6 AS Good`
	expected := new(netmap.PlacementPolicy)
	expected.SetReplicas([]*netmap.Replica{newReplica("", 1)})
	expected.SetSelectors([]*netmap.Selector{
		newSelector(2, netmap.UnspecifiedClause, "City", "Good", ""),
	})
	expected.SetFilters([]*netmap.Filter{
		newFilter("Good", "", "", netmap.AND,
			newFilter("", "A", "1", netmap.GT),
			newFilter("", "B", "2", netmap.GE),
			newFilter("", "C", "3", netmap.LT),
			newFilter("", "D", "4", netmap.LE),
			newFilter("", "E", "5", netmap.EQ),
			newFilter("", "F", "6", netmap.NE)),
	})

	r, err := Parse(q)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestWithFilterPrecedence(t *testing.T) {
	q := `REP 7 IN SPB
SELECT 1 IN City FROM SPBSSD AS SPB
FILTER City EQ "SPB" AND SSD EQ true OR City EQ "SPB" AND Rating GE 5 AS SPBSSD`
	expected := new(netmap.PlacementPolicy)
	expected.SetReplicas([]*netmap.Replica{newReplica("SPB", 7)})
	expected.SetSelectors([]*netmap.Selector{
		newSelector(1, netmap.UnspecifiedClause, "City", "SPBSSD", "SPB"),
	})
	expected.SetFilters([]*netmap.Filter{
		newFilter("SPBSSD", "", "", netmap.OR,
			newFilter("", "", "", netmap.AND,
				newFilter("", "City", "SPB", netmap.EQ),
				newFilter("", "SSD", "true", netmap.EQ)),
			newFilter("", "", "", netmap.AND,
				newFilter("", "City", "SPB", netmap.EQ),
				newFilter("", "Rating", "5", netmap.GE))),
	})

	r, err := Parse(q)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestValidation(t *testing.T) {
	t.Run("MissingSelector", func(t *testing.T) {
		q := `REP 3 IN RU`
		_, err := Parse(q)
		require.True(t, errors.Is(err, ErrUnknownSelector), "got: %v", err)
	})
	t.Run("MissingFilter", func(t *testing.T) {
		q := `REP 3
              SELECT 1 IN City FROM MissingFilter`
		_, err := Parse(q)
		require.True(t, errors.Is(err, ErrUnknownFilter), "got: %v", err)
	})
	t.Run("UnknownOp", func(t *testing.T) {
		q := `REP 3
              SELECT 1 IN City FROM F
			  FILTER Country KEK RU AS F`
		_, err := Parse(q)
		require.True(t, errors.Is(err, ErrUnknownOp), "got: %v", err)
	})
	t.Run("TypoInREP", func(t *testing.T) {
		q := `REK 3`
		_, err := Parse(q)
		require.Error(t, err)
	})
	t.Run("InvalidFilterName", func(t *testing.T) {
		q := `REP 3
			  SELECT 1 IN City FROM F
			  FILTER Good AND Country EQ RU AS F
			  FILTER Rating EQ 5 AS Good`
		_, err := Parse(q)
		require.Error(t, err)
	})
	t.Run("InvalidNumberInREP", func(t *testing.T) {
		q := `REP 0`
		_, err := Parse(q)
		require.True(t, errors.Is(err, ErrInvalidNumber), "got: %v", err)
	})
	t.Run("InvalidNumberInREP", func(t *testing.T) {
		q := `REP 1 IN Good
			  SELECT 0 IN City FROM *`
		_, err := Parse(q)
		require.True(t, errors.Is(err, ErrInvalidNumber), "got: %v", err)
	})

}

func newFilter(name, key, value string, op netmap.Operation, sub ...*netmap.Filter) *netmap.Filter {
	f := new(netmap.Filter)
	f.SetName(name)
	f.SetKey(key)
	f.SetValue(value)
	f.SetOp(op)
	f.SetFilters(sub)
	return f
}

func newReplica(s string, c uint32) *netmap.Replica {
	r := new(netmap.Replica)
	r.SetSelector(s)
	r.SetCount(c)
	return r
}

func newSelector(count uint32, c netmap.Clause, attr, f, name string) *netmap.Selector {
	s := new(netmap.Selector)
	s.SetCount(count)
	s.SetClause(c)
	s.SetAttribute(attr)
	s.SetFilter(f)
	s.SetName(name)
	return s
}
